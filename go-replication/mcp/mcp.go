// Package mcp 是 MCP 集成 —— 对应 deer-flow mcp/ 目录下的 5 个文件:
//
//   - client.py::build_server_params / build_servers_config(transport 翻译)
//   - session_pool.py::MCPSessionPool(持久会话池 + LRU + owner-task + in-flight 去重)
//   - cache.py(MCP 工具缓存:惰性加载 + config mtime 失效)
//   - oauth.py::OAuthTokenManager(双检锁续期)
//   - tools.py::_make_session_pool_tool(会话池的消费者:scope_key + stdio 隔离 + 拦截器链)
//
// 三个核心设计(和 deer-flow 一致):
//   - 配置翻译:transport(stdio/sse/http)翻译成连接参数(stdio 要 command,http/sse 要 url)。
//   - 持久会话池:按 (server, scope) 隔离,复用会话以保留服务器端状态(如 Playwright 浏览器),
//     MAX_SESSIONS=256 + LRU 淘汰 + in-flight 创建去重 + 优雅关闭。
//   - 惰性加载 + mtime 失效:工具缓存按配置文件 mtime 判断是否过期,过期即重建。
//
// Go 与 Python 的关键差异(session_pool 的复杂度来源):
//   - Python 的 owner-task 模式是为了满足 anyio「cancel scope 必须在进入它的同一任务
//     退出」约束;Go 的 goroutine + channel 没有这个约束,「谁创建谁关闭」天然成立,
//     所以整个会话池只需一个 owner goroutine 等 close 信号。跨 loop 关闭路由
//     (_shutdown_entry / call_soon_threadsafe / run_coroutine_threadsafe)在 Go 里
//     全部退化成「signal + 等 done」。
//   - Python 用 task.cancel() 打断阻塞在 initialize() 的 in-flight owner;Go 里用
//     context.CancelFunc 取消传给 factory.Open 的 ctx 达到等价效果。
//   - 会话的 event-loop 身份(loop is current_loop / loop.is_closed)在 Go 里不存在,
//     get_session 的「跨 loop 驱逐」与「stale in-flight」分支随之消失(见注释)。
package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"deerflow-go/capability"
)

// logger 是包级日志(对应 Python 的 logging.getLogger(__name__))。
var logger = log.New(os.Stderr, "mcp: ", log.LstdFlags)

// ---------------------------------------------------------------------------
// 配置类型
// ---------------------------------------------------------------------------

// McpServerConfig 是一个 MCP server 的配置(client.py 里 config 的形态)。
type McpServerConfig struct {
	Type    string // "stdio" / "sse" / "http"(空 = stdio)
	Command string
	Args    []string
	Env     map[string]string
	URL     string
	Headers map[string]string
	OAuth   *McpOAuthConfig
}

// ExtensionsConfig 是扩展配置(含所有 MCP server 与拦截器)。
type ExtensionsConfig struct {
	MCPServers map[string]McpServerConfig
	// Interceptors 对应 extensions_config.json 的 "mcpInterceptors"。
	Interceptors []string
	Extra        map[string]any // model_extra
}

// GetEnabledMCPServers 返回已启用的 MCP server。deer-flow 里「enabled」由配置里的
// enabled 标记决定;这里约定放进 map 的即视为启用。
func (c *ExtensionsConfig) GetEnabledMCPServers() map[string]McpServerConfig {
	return c.MCPServers
}

// ---------------------------------------------------------------------------
// client.py
// ---------------------------------------------------------------------------

// BuildServerParams 把 transport 配置翻译成连接参数(client.py:11-42)。
//
//   - stdio:必须给 command;args 总是写入(即使为空,对应 Python 无条件 params["args"])。
//   - sse/http:必须给 url;headers 可选。
//   - 其它 transport:报错。
func BuildServerParams(serverName string, cfg McpServerConfig) (map[string]any, error) {
	transportType := cfg.Type
	if transportType == "" {
		transportType = "stdio"
	}
	params := map[string]any{"transport": transportType}

	switch transportType {
	case "stdio":
		if cfg.Command == "" {
			return nil, fmt.Errorf("MCP server '%s' with stdio transport requires 'command' field", serverName)
		}
		params["command"] = cfg.Command
		params["args"] = cfg.Args
		if cfg.Env != nil {
			params["env"] = cfg.Env
		}
	case "sse", "http":
		if cfg.URL == "" {
			return nil, fmt.Errorf("MCP server '%s' with %s transport requires 'url' field", serverName, transportType)
		}
		params["url"] = cfg.URL
		if cfg.Headers != nil {
			params["headers"] = cfg.Headers
		}
	default:
		return nil, fmt.Errorf("MCP server '%s' has unsupported transport type: %s", serverName, transportType)
	}
	return params, nil
}

// BuildServersConfig 为所有启用 server 构建连接配置(client.py:45-68)。
// 单个 server 配置失败只记日志跳过,不影响其它 server(对应 Python 的 try/except per server)。
func BuildServersConfig(ext ExtensionsConfig) map[string]map[string]any {
	enabled := ext.GetEnabledMCPServers()
	if len(enabled) == 0 {
		return map[string]map[string]any{}
	}
	serversConfig := map[string]map[string]any{}
	for serverName, cfg := range enabled {
		params, err := BuildServerParams(serverName, cfg)
		if err != nil {
			logger.Printf("Failed to configure MCP server '%s': %v", serverName, err)
			continue
		}
		serversConfig[serverName] = params
	}
	return serversConfig
}

// ---------------------------------------------------------------------------
// session_pool.py
// ---------------------------------------------------------------------------

// MaxSessions 是会话池容量上限(session_pool.py:50 的 MAX_SESSIONS)。
const MaxSessions = 256

// SessionCloseTimeout 是关闭会话时等待 owner goroutine 收尾的最长时间
// (session_pool.py:51 的 SESSION_CLOSE_TIMEOUT)。
const SessionCloseTimeout = 5 * time.Second

// Session 是一个活的 MCP 会话(连接 + 服务器端状态)。真实实现是 MCP ClientSession。
type Session struct {
	ID         string
	Connection map[string]any
	callTool   func(ctx context.Context, name string, args map[string]any, opts map[string]any) (*CallToolResult, error)
}

// CallTool 调用会话里的一个工具(真实实现 = ClientSession.call_tool)。
func (s *Session) CallTool(ctx context.Context, name string, args map[string]any, opts map[string]any) (*CallToolResult, error) {
	if s.callTool == nil {
		return nil, errors.New("session has no call_tool bound")
	}
	return s.callTool(ctx, name, args, opts)
}

// SessionFactory 是会话生命周期钩子。真实实现 = langchain-mcp-adapters 的
// create_session + ClientSession.initialize + context-manager __aexit__。
type SessionFactory interface {
	// Open 建立并初始化一个会话,ctx 取消时应尽快返回(等价 cancel 打断 initialize)。
	Open(ctx context.Context, connection map[string]any) (*Session, error)
	// Close 拆除会话(幂等)。
	Close(s *Session)
}

// sessionKey 是 (server, scope) 隔离键(session_pool.py 的 tuple key)。
type sessionKey struct {
	server string
	scope  string
}

// owner 是一个会话的「owner goroutine」控制面:
//   - closeCh 被关闭 = 请 owner 关闭会话(Python close_event.set,幂等)。
//   - done 在 owner 完成收尾后关闭(Python owner_task 结束)。
type owner struct {
	closeCh   chan struct{}
	closeOnce sync.Once
	done      chan struct{}
}

// signal 幂等地发出关闭信号(对应 asyncio.Event.set 可重复调用)。
func (o *owner) signal() {
	o.closeOnce.Do(func() { close(o.closeCh) })
}

// poolEntry 是已注册(已初始化)的池条目。
type poolEntry struct {
	session *Session
	owner   *owner
}

// inflightEntry 是「创建中」的池条目。cancel 用于打断阻塞在 Open 里的 owner
// (对应 Python 对 in-flight task.cancel())。
type inflightEntry struct {
	owner  *owner
	cancel context.CancelFunc
	ready  *readySignal
}

// ownerResult 是 owner goroutine 回传的结果。
type ownerResult struct {
	session *Session
	err     error
}

// readySignal 是「多消费者」的 ready 信号:Python 里 creator 与 joiners 都
// `await shield(join)` 同一个 Future,人人拿同一结果;Go 的 channel 是单消费者的,
// 所以用「关闭 done 通道广播 + 存储结果」复现多消费者语义。
type readySignal struct {
	done   chan struct{}
	once   sync.Once
	result ownerResult
}

func newReadySignal() *readySignal {
	return &readySignal{done: make(chan struct{})}
}

// set 发布结果并广播(幂等,写 result 先于 close(done),依赖 happens-before)。
func (r *readySignal) set(res ownerResult) {
	r.result = res
	r.once.Do(func() { close(r.done) })
}

// wait 等结果或 ctx 取消。返回的 error 仅当 ctx 先于结果取消时非 nil。
func (r *readySignal) wait(ctx context.Context) (ownerResult, error) {
	select {
	case <-r.done:
		return r.result, nil
	default:
	}
	select {
	case <-r.done:
		return r.result, nil
	case <-ctx.Done():
		return ownerResult{}, ctx.Err()
	}
}

// SessionPool 管理按 (server, scope) 隔离的持久 MCP 会话。
type SessionPool struct {
	mu       sync.Mutex
	entries  map[sessionKey]*poolEntry
	order    []sessionKey // LRU 顺序,最旧在前
	inflight map[sessionKey]*inflightEntry
	max      int
	factory  SessionFactory
}

// NewSessionPool 构造会话池。factory 为 nil 时用 DefaultSessionFactory。
func NewSessionPool(factory SessionFactory) *SessionPool {
	if factory == nil {
		factory = DefaultSessionFactory
	}
	return &SessionPool{
		entries:  map[sessionKey]*poolEntry{},
		inflight: map[sessionKey]*inflightEntry{},
		max:      MaxSessions,
		factory:  factory,
	}
}

// runSession 是 owner goroutine:进入会话、初始化、发布 ready,然后阻塞等 close 信号,
// 最后在同一 goroutine 里 Close(满足「谁创建谁关闭」)。
func (p *SessionPool) runSession(ctx context.Context, connection map[string]any, ready *readySignal, o *owner) {
	s, err := p.factory.Open(ctx, connection)
	if err != nil {
		// 从未成功创建会话,没有需要关闭的资源。
		ready.set(ownerResult{err: err})
		close(o.done)
		return
	}
	// 会话已创建:从此刻起 Close 必须在本 goroutine 执行。
	ready.set(ownerResult{session: s})
	<-o.closeCh
	p.factory.Close(s)
	close(o.done)
}

// GetSession 获取或创建一个持久 MCP 会话(session_pool.py:126-263)。
//
// 三阶段:锁内原子决定「复用 / 加入 in-flight / 成为创建者 + LRU 驱逐」→ 关掉被驱逐者 →
// 等 ready 后把 in-flight 提升为正式条目(仅当仍是我们的记录)。
//
// Go 里没有 event-loop 身份,故省去 Python 的「跨 loop 驱逐」与「stale in-flight」两分支;
// in-flight 去重、LRU、优雅关闭语义完整保留。
func (p *SessionPool) GetSession(ctx context.Context, server, scope string, connection map[string]any) (*Session, error) {
	key := sessionKey{server: server, scope: scope}

	var evicted []*owner
	var join *inflightEntry
	var mine *inflightEntry

	p.mu.Lock()
	if e, ok := p.entries[key]; ok {
		// 复用已有会话(Go 里不存在「别的 loop 上创建的会话」,无需驱逐)。
		p.touchLocked(key)
		p.mu.Unlock()
		return e.session, nil
	}

	if inf, ok := p.inflight[key]; ok {
		// 另一个调用方正在创建同一 key 的会话:加入而不是重复创建。
		join = inf
	} else {
		// 成为创建者:在任何阻塞前先发布 in-flight 记录,让并发调用方 join 我们。
		o := &owner{closeCh: make(chan struct{}), done: make(chan struct{})}
		ready := newReadySignal()
		ownerCtx, cancel := context.WithCancel(context.Background())
		mine = &inflightEntry{owner: o, cancel: cancel, ready: ready}
		go p.runSession(ownerCtx, connection, ready, o)
		p.inflight[key] = mine
	}

	// LRU 驱逐:达到容量(>= MAX_SESSIONS)时关掉最久未用的会话。
	// 注意此驱逐在 join 与 create 两条路径都会执行(与 Python 的 while 位置一致)。
	for len(p.entries) >= p.max {
		oldestKey := p.order[0]
		p.order = p.order[1:]
		if e, ok := p.entries[oldestKey]; ok {
			delete(p.entries, oldestKey)
			evicted = append(evicted, e.owner)
		}
	}
	p.mu.Unlock()

	// 阶段 2:关掉被驱逐的 owner(gentle close_evt 路径)。
	for _, o := range evicted {
		p.shutdownOwner(o, nil)
	}

	// 阶段 2b:同一 key 的并发创建已在进行 —— 共享其结果(多消费者,人人拿同一结果)。
	if join != nil {
		res, waitErr := join.ready.wait(ctx)
		if waitErr != nil {
			return nil, waitErr
		}
		return res.session, res.err
	}

	// 阶段 3:等我们自己的 owner 发布 ready。
	res, waitErr := mine.ready.wait(ctx)
	if waitErr != nil {
		// 本次调用被取消:信号关闭 + 取消 owner,等它收尾,清掉 in-flight 记录。
		mine.cancel()
		mine.owner.signal()
		<-mine.owner.done
		p.mu.Lock()
		if p.inflight[key] == mine {
			delete(p.inflight, key)
		}
		p.mu.Unlock()
		return nil, waitErr
	}
	if res.err != nil {
		// owner 在 connect/initialize 阶段失败:它已在自己的 goroutine 里收尾,
		// 只需等它结束并清记录。
		<-mine.owner.done
		p.mu.Lock()
		if p.inflight[key] == mine {
			delete(p.inflight, key)
		}
		p.mu.Unlock()
		return nil, res.err
	}

	// 阶段 4:把 in-flight 提升为正式条目 —— 仅当它仍是「活的」记录。
	// 若初始化期间被 close_scope/close_server/close_all 移除,则不能复活,改为自行拆除。
	p.mu.Lock()
	stillOurs := p.inflight[key] == mine
	if stillOurs {
		delete(p.inflight, key)
		p.entries[key] = &poolEntry{session: res.session, owner: mine.owner}
		p.order = append(p.order, key)
	}
	p.mu.Unlock()
	if !stillOurs {
		p.shutdownOwner(mine.owner, nil)
		return nil, errors.New("MCP session pool was closed while the session was being created")
	}
	return res.session, nil
}

// shutdownOwner 发出关闭信号并等待 owner 收尾(超时则记日志,不无限阻塞)。
func (p *SessionPool) shutdownOwner(o *owner, cancel context.CancelFunc) {
	if cancel != nil {
		cancel()
	}
	o.signal()
	select {
	case <-o.done:
	case <-time.After(SessionCloseTimeout):
		logger.Printf("timed out waiting for MCP session owner to close")
	}
}

// touchLocked 把 key 移到 LRU 末尾(最近使用)。调用方须持锁。
func (p *SessionPool) touchLocked(key sessionKey) {
	for i, k := range p.order {
		if k == key {
			p.order = append(p.order[:i], p.order[i+1:]...)
			p.order = append(p.order, key)
			return
		}
	}
}

// CloseScope 关闭某 scope(如 thread_id)下的所有会话(session_pool.py:342-352)。
func (p *SessionPool) CloseScope(scopeKey string) {
	p.mu.Lock()
	var entries []*poolEntry
	var inflights []*inflightEntry
	for k, e := range p.entries {
		if k.scope == scopeKey {
			entries = append(entries, e)
			delete(p.entries, k)
		}
	}
	for k, inf := range p.inflight {
		if k.scope == scopeKey {
			inflights = append(inflights, inf)
			delete(p.inflight, k)
		}
	}
	p.order = filterOrder(p.order, func(k sessionKey) bool { return k.scope == scopeKey })
	p.mu.Unlock()

	for _, e := range entries {
		p.shutdownOwner(e.owner, nil)
	}
	for _, inf := range inflights {
		p.shutdownOwner(inf.owner, inf.cancel)
	}
}

// CloseServer 关闭某 server 下的所有会话(session_pool.py:354-364)。
func (p *SessionPool) CloseServer(serverName string) {
	p.mu.Lock()
	var entries []*poolEntry
	var inflights []*inflightEntry
	for k, e := range p.entries {
		if k.server == serverName {
			entries = append(entries, e)
			delete(p.entries, k)
		}
	}
	for k, inf := range p.inflight {
		if k.server == serverName {
			inflights = append(inflights, inf)
			delete(p.inflight, k)
		}
	}
	p.order = filterOrder(p.order, func(k sessionKey) bool { return k.server == serverName })
	p.mu.Unlock()

	for _, e := range entries {
		p.shutdownOwner(e.owner, nil)
	}
	for _, inf := range inflights {
		p.shutdownOwner(inf.owner, inf.cancel)
	}
}

// CloseAll 关闭所有会话(session_pool.py:366-376 的 async close_all)。
func (p *SessionPool) CloseAll() {
	p.closeAll()
}

// CloseAllSync 关闭所有会话(session_pool.py:378-431 的 close_all_sync)。
//
// Python 里 sync/async 关闭要区分「owning loop 是否当前运行 loop」以避自死锁;
// Go 里关闭总是由「非 owner 的调用 goroutine」执行,没有这个约束,两者等价。
func (p *SessionPool) CloseAllSync() {
	p.closeAll()
}

func (p *SessionPool) closeAll() {
	p.mu.Lock()
	entries := make([]*poolEntry, 0, len(p.entries))
	for _, e := range p.entries {
		entries = append(entries, e)
	}
	p.entries = map[sessionKey]*poolEntry{}
	inflights := make([]*inflightEntry, 0, len(p.inflight))
	for _, inf := range p.inflight {
		inflights = append(inflights, inf)
	}
	p.inflight = map[sessionKey]*inflightEntry{}
	p.order = nil
	p.mu.Unlock()

	for _, e := range entries {
		p.shutdownOwner(e.owner, nil)
	}
	for _, inf := range inflights {
		p.shutdownOwner(inf.owner, inf.cancel)
	}
}

// Len 返回当前会话数(观测用)。
func (p *SessionPool) Len() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.entries)
}

func filterOrder(order []sessionKey, drop func(sessionKey) bool) []sessionKey {
	out := make([]sessionKey, 0, len(order))
	for _, k := range order {
		if !drop(k) {
			out = append(out, k)
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// session_pool.py 模块级单例
// ---------------------------------------------------------------------------

var (
	globalPoolMu sync.Mutex
	globalPool   *SessionPool
)

// GetSessionPool 返回全局会话池单例(session_pool.py:442-449 的双检锁)。
func GetSessionPool() *SessionPool {
	globalPoolMu.Lock()
	defer globalPoolMu.Unlock()
	if globalPool == nil {
		globalPool = NewSessionPool(DefaultSessionFactory)
	}
	return globalPool
}

// ResetSessionPool 重置单例(测试用,session_pool.py:452-455)。
func ResetSessionPool() {
	globalPoolMu.Lock()
	defer globalPoolMu.Unlock()
	if globalPool != nil {
		globalPool.CloseAll()
	}
	globalPool = nil
}

// DefaultSessionFactory 是默认会话工厂:不真正连接,仅按 connection 生成一个会话
// (真实 stdio/HTTP 子进程工厂超出本包范围,通过 SessionFactory 注入)。
var DefaultSessionFactory SessionFactory = memorySessionFactory{}

type memorySessionFactory struct{}

func (memorySessionFactory) Open(_ context.Context, connection map[string]any) (*Session, error) {
	return &Session{ID: fmt.Sprintf("session-%d", time.Now().UnixNano()), Connection: connection}, nil
}

func (memorySessionFactory) Close(*Session) {}

// ---------------------------------------------------------------------------
// cache.py
// ---------------------------------------------------------------------------

// ToolCache 是 MCP 工具缓存:惰性加载 + config 文件 mtime 失效(cache.py)。
type ToolCache struct {
	mu          sync.Mutex
	cache       []capability.Tool
	initialized bool
	configMtime *time.Time // nil = 未知
	loader      func() ([]capability.Tool, error)
	mtimeFn     func() *time.Time
	onReset     func()
}

// NewToolCache 构造缓存。loader 加载工具,mtimeFn 取配置文件 mtime,onReset 在重置时
// 执行副作用(关闭会话池)。三者可注入,便于测试。
func NewToolCache(loader func() ([]capability.Tool, error), mtimeFn func() *time.Time, onReset func()) *ToolCache {
	return &ToolCache{loader: loader, mtimeFn: mtimeFn, onReset: onReset}
}

// snapshotMtime 取配置文件 mtime 的「值快照」。
// mtimeFn 可能返回指向共享变量的指针(测试里常见),必须解引用存值,
// 否则缓存里存的是指针,外部一改值,快照就跟着变,isStale 永远为 false。
// 对应 cache.py 里 mtime 是 float 值语义(存的是当时的值,不是引用)。
func (c *ToolCache) snapshotMtime() *time.Time {
	cur := c.mtimeFn()
	if cur == nil {
		return nil
	}
	v := *cur
	return &v
}

// isStale 判断缓存是否因配置文件变化而失效(cache.py:31-53 的 _is_cache_stale)。
func (c *ToolCache) isStale() bool {
	if !c.initialized {
		return false // 未初始化,谈不上过期
	}
	if c.configMtime == nil {
		return false
	}
	current := c.mtimeFn()
	if current == nil {
		return false
	}
	return current.After(*c.configMtime)
}

// Initialize 启动时加载并缓存工具(cache.py:56-79 的 initialize_mcp_tools,幂等)。
func (c *ToolCache) Initialize() ([]capability.Tool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.initialized {
		return c.cache, nil
	}
	tools, err := c.loader()
	if err != nil {
		return nil, err
	}
	c.cache = tools
	c.initialized = true
	c.configMtime = c.snapshotMtime()
	return c.cache, nil
}

// GetCached 获取缓存工具,惰性加载(cache.py:82-129 的 get_cached_mcp_tools)。
// 惰性加载失败返回空列表(对应 Python 的 except → return [])。
func (c *ToolCache) GetCached() []capability.Tool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.isStale() {
		c.resetLocked()
	}
	if !c.initialized {
		tools, err := c.loader()
		if err != nil {
			return []capability.Tool{}
		}
		c.cache = tools
		c.initialized = true
		c.configMtime = c.snapshotMtime()
	}
	if c.cache == nil {
		return []capability.Tool{}
	}
	return c.cache
}

// Reset 重置缓存(cache.py:132-166 的 reset_mcp_tools_cache,并关闭会话池)。
func (c *ToolCache) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resetLocked()
}

func (c *ToolCache) resetLocked() {
	c.cache = nil
	c.initialized = false
	c.configMtime = nil
	if c.onReset != nil {
		c.onReset()
	}
}

// ---------------------------------------------------------------------------
// oauth.py
// ---------------------------------------------------------------------------

// McpOAuthConfig 是一个 MCP server 的 OAuth 配置(oauth.py 里 McpOAuthConfig 的形态)。
type McpOAuthConfig struct {
	Enabled            bool
	TokenURL           string
	GrantType          string // client_credentials / refresh_token
	ClientID           string
	ClientSecret       string
	RefreshToken       string
	Scope              string
	Audience           string
	ExtraTokenParams   map[string]string
	TokenField         string // 默认 access_token
	TokenTypeField     string // 默认 token_type
	ExpiresInField     string // 默认 expires_in
	DefaultTokenType   string // 默认 Bearer
	RefreshSkewSeconds int
}

// OAuthToken 是缓存的 OAuth token(oauth.py::_OAuthToken)。
type OAuthToken struct {
	AccessToken string
	TokenType   string
	ExpiresAt   time.Time
}

// OAuthTokenManager 获取/缓存/续期 MCP server 的 OAuth token(oauth.py::OAuthTokenManager)。
type OAuthTokenManager struct {
	mu            sync.Mutex
	oauthByServer map[string]McpOAuthConfig
	tokens        map[string]*OAuthToken
	locks         map[string]*sync.Mutex
	now           func() time.Time
	post          func(ctx context.Context, url string, data map[string]string) (map[string]any, error)
}

// NewOAuthTokenManager 构造 manager。每个 server 一把独立锁(对应 oauth.py 的 per-server asyncio.Lock)。
func NewOAuthTokenManager(oauthByServer map[string]McpOAuthConfig) *OAuthTokenManager {
	locks := map[string]*sync.Mutex{}
	for name := range oauthByServer {
		locks[name] = &sync.Mutex{}
	}
	return &OAuthTokenManager{
		oauthByServer: oauthByServer,
		tokens:        map[string]*OAuthToken{},
		locks:         locks,
		now:           time.Now,
		post:          postOAuthToken,
	}
}

// OAuthTokenManagerFromExtensionsConfig 从扩展配置构造 manager(oauth.py:33-39)。
func OAuthTokenManagerFromExtensionsConfig(ext ExtensionsConfig) *OAuthTokenManager {
	oauthByServer := map[string]McpOAuthConfig{}
	for name, cfg := range ext.GetEnabledMCPServers() {
		if cfg.OAuth != nil && cfg.OAuth.Enabled {
			oauthByServer[name] = *cfg.OAuth
		}
	}
	return NewOAuthTokenManager(oauthByServer)
}

// HasOAuthServers 是否有启用 OAuth 的 server(oauth.py:41-42)。
func (m *OAuthTokenManager) HasOAuthServers() bool { return len(m.oauthByServer) > 0 }

// OAuthServerNames 返回启用 OAuth 的 server 名(oauth.py:44-45)。
func (m *OAuthTokenManager) OAuthServerNames() []string {
	names := make([]string, 0, len(m.oauthByServer))
	for name := range m.oauthByServer {
		names = append(names, name)
	}
	return names
}

// GetAuthorizationHeader 返回某 server 的 Authorization 头(oauth.py:47-65)。
//
// 双检锁:先无锁快查,未命中再进 per-server 锁,锁内再查一次(避免并发重复续期)。
// 无该 server 的 OAuth 配置时返回空串 + nil error(对应 Python 返回 None)。
func (m *OAuthTokenManager) GetAuthorizationHeader(ctx context.Context, serverName string) (string, error) {
	oauth, ok := m.oauthByServer[serverName]
	if !ok {
		return "", nil
	}

	if tok := m.tokens[serverName]; tok != nil && !m.isExpiring(tok, oauth) {
		return tok.TokenType + " " + tok.AccessToken, nil
	}

	lock := m.locks[serverName]
	lock.Lock()
	defer lock.Unlock()

	if tok := m.tokens[serverName]; tok != nil && !m.isExpiring(tok, oauth) {
		return tok.TokenType + " " + tok.AccessToken, nil
	}

	fresh, err := m.fetchToken(ctx, oauth)
	if err != nil {
		return "", err
	}
	m.tokens[serverName] = fresh
	return fresh.TokenType + " " + fresh.AccessToken, nil
}

// isExpiring 对应 oauth.py:67-70 的 _is_expiring:到期时间 <= now + refresh_skew 即视为将过期。
func (m *OAuthTokenManager) isExpiring(tok *OAuthToken, oauth McpOAuthConfig) bool {
	skew := oauth.RefreshSkewSeconds
	if skew < 0 {
		skew = 0
	}
	return !tok.ExpiresAt.After(m.now().Add(time.Duration(skew) * time.Second))
}

func (m *OAuthTokenManager) fetchToken(ctx context.Context, oauth McpOAuthConfig) (*OAuthToken, error) {
	data, err := buildTokenRequestData(oauth)
	if err != nil {
		return nil, err
	}
	payload, err := m.post(ctx, oauth.TokenURL, data)
	if err != nil {
		return nil, err
	}
	return parseTokenResponse(payload, oauth, m.now())
}

// buildTokenRequestData 构建 token 请求的表单数据(oauth.py:72-99 的 _fetch_token 前半段)。
func buildTokenRequestData(oauth McpOAuthConfig) (map[string]string, error) {
	data := map[string]string{"grant_type": oauth.GrantType}
	for k, v := range oauth.ExtraTokenParams {
		data[k] = v
	}
	if oauth.Scope != "" {
		data["scope"] = oauth.Scope
	}
	if oauth.Audience != "" {
		data["audience"] = oauth.Audience
	}

	switch oauth.GrantType {
	case "client_credentials":
		if oauth.ClientID == "" || oauth.ClientSecret == "" {
			return nil, errors.New("OAuth client_credentials requires client_id and client_secret")
		}
		data["client_id"] = oauth.ClientID
		data["client_secret"] = oauth.ClientSecret
	case "refresh_token":
		if oauth.RefreshToken == "" {
			return nil, errors.New("OAuth refresh_token grant requires refresh_token")
		}
		data["refresh_token"] = oauth.RefreshToken
		if oauth.ClientID != "" {
			data["client_id"] = oauth.ClientID
		}
		if oauth.ClientSecret != "" {
			data["client_secret"] = oauth.ClientSecret
		}
	default:
		return nil, fmt.Errorf("Unsupported OAuth grant type: %s", oauth.GrantType)
	}
	return data, nil
}

// parseTokenResponse 解析 token 响应(oauth.py:106-119 的 _fetch_token 后半段)。
func parseTokenResponse(payload map[string]any, oauth McpOAuthConfig, now time.Time) (*OAuthToken, error) {
	tokenField := oauth.TokenField
	if tokenField == "" {
		tokenField = "access_token"
	}
	accessToken, _ := payload[tokenField].(string)
	if accessToken == "" {
		return nil, fmt.Errorf("OAuth token response missing '%s'", tokenField)
	}

	tokenTypeField := oauth.TokenTypeField
	if tokenTypeField == "" {
		tokenTypeField = "token_type"
	}
	defaultType := oauth.DefaultTokenType
	if defaultType == "" {
		defaultType = "Bearer"
	}
	tokenType := asString(payload[tokenTypeField])
	if tokenType == "" {
		tokenType = defaultType
	}

	expiresInField := oauth.ExpiresInField
	if expiresInField == "" {
		expiresInField = "expires_in"
	}
	expiresIn := 3600
	if v, ok := payload[expiresInField]; ok {
		if n, ok2 := toIntSeconds(v); ok2 {
			expiresIn = n
		}
	}
	if expiresIn < 1 {
		expiresIn = 1
	}

	return &OAuthToken{
		AccessToken: accessToken,
		TokenType:   tokenType,
		ExpiresAt:   now.Add(time.Duration(expiresIn) * time.Second),
	}, nil
}

// postOAuthToken 用 form 编码 POST 到 token_url(oauth.py:101-104,timeout 15s + raise_for_status)。
func postOAuthToken(ctx context.Context, tokenURL string, data map[string]string) (map[string]any, error) {
	form := url.Values{}
	for k, v := range data {
		form.Set(k, v)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, tokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("OAuth token request failed: %s", resp.Status)
	}
	var payload map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func asString(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	default:
		return fmt.Sprintf("%v", t)
	}
}

func toIntSeconds(v any) (int, bool) {
	switch n := v.(type) {
	case int:
		return n, true
	case int64:
		return int(n), true
	case float64:
		return int(n), true
	case json.Number:
		if i, err := n.Int64(); err == nil {
			return int(i), true
		}
	case string:
		if i, err := strconv.Atoi(n); err == nil {
			return i, true
		}
	}
	return 0, false
}

// ---------------------------------------------------------------------------
// oauth.py 的拦截器 / 初始头
// ---------------------------------------------------------------------------

// ToolCallRequest 是工具拦截器的请求(oauth.py 里 request 的形态)。
type ToolCallRequest struct {
	Name       string
	Args       map[string]any
	ServerName string
	Headers    map[string]string
}

// CallToolResult 是 MCP call_tool 的返回值(简化自 mcp.types.CallToolResult)。
type CallToolResult struct {
	Content           []ContentBlock
	IsError           bool
	StructuredContent map[string]any
}

// ContentBlock 是 MCP 内容块(TextContent/ImageContent/ResourceLink/EmbeddedResource 的简化)。
type ContentBlock struct {
	Type     string // "text" / "image" / "resource_link" / "embedded_resource"
	Text     string
	Data     string // base64
	MimeType string
	URI      string
}

// ToolInterceptor 是工具拦截器(oauth.py 的 oauth_interceptor 泛化)。
type ToolInterceptor func(req ToolCallRequest, handler func(ToolCallRequest) (*CallToolResult, error)) (*CallToolResult, error)

// BuildOAuthToolInterceptor 构造注入 OAuth Authorization 头的拦截器(oauth.py:122-137)。
func BuildOAuthToolInterceptor(m *OAuthTokenManager) ToolInterceptor {
	return func(req ToolCallRequest, handler func(ToolCallRequest) (*CallToolResult, error)) (*CallToolResult, error) {
		header, err := m.GetAuthorizationHeader(context.Background(), req.ServerName)
		if err != nil {
			return nil, err
		}
		if header == "" {
			return handler(req)
		}
		updated := map[string]string{}
		for k, v := range req.Headers {
			updated[k] = v
		}
		updated["Authorization"] = header
		req.Headers = updated
		return handler(req)
	}
}

// GetInitialOAuthHeaders 获取 server 连接的初始 OAuth 头(oauth.py:140-150)。
func GetInitialOAuthHeaders(ctx context.Context, ext ExtensionsConfig) (map[string]string, error) {
	m := OAuthTokenManagerFromExtensionsConfig(ext)
	if !m.HasOAuthServers() {
		return map[string]string{}, nil
	}
	headers := map[string]string{}
	for _, name := range m.OAuthServerNames() {
		h, err := m.GetAuthorizationHeader(ctx, name)
		if err != nil {
			return nil, err
		}
		if h != "" {
			headers[name] = h
		}
	}
	return headers, nil
}

// ---------------------------------------------------------------------------
// tools.py:_make_session_pool_tool(会话池消费者)
// ---------------------------------------------------------------------------

// Runtime 是工具调用注入的运行时上下文(tools.py 里 Runtime 的形态)。
type Runtime struct {
	Context map[string]any
	Config  map[string]any
}

// Workspace 抽象 stdio 子进程的宿主路径(对应 get_paths())。
// 仅暴露会话池消费所必需的两个方法;完整 sandbox 路径由 sandbox 子系统提供。
type Workspace interface {
	// WorkDir 返回某 (thread, user) 的工作目录(stdio 子进程 cwd)。
	WorkDir(threadID, userID string) string
	// VirtualPath 把本地绝对路径映射成 /mnt/user-data/... 虚拟路径;无法映射返回 false。
	VirtualPath(localPath, threadID, userID string) (string, bool)
}

// mcpTmpSubdir 是 thread 工作区下 stdio 子进程的临时目录(tools.py:33 的 _MCP_TMP_SUBDIR)。
const mcpTmpSubdir = ".mcp/tmp"

// ScopeKey 拼出会话隔离键(tools.py:445 的 f"{user_id}:{thread_id}")。
// 文件系统隔离是 per-(user_id, thread_id),仅按 thread_id 会让碰撞 thread_id 的
// 两个用户共享同一有状态会话,故必须带 user_id。
func ScopeKey(userID, threadID string) string {
	return userID + ":" + threadID
}

// ExtractThreadID 从 runtime 提取 thread_id(tools.py:291-306 的 _extract_thread_id)。
func ExtractThreadID(runtime *Runtime) string {
	if runtime != nil {
		if runtime.Context != nil {
			if tid, ok := runtime.Context["thread_id"]; ok && tid != nil {
				return fmt.Sprintf("%v", tid)
			}
		}
		if runtime.Config != nil {
			if cfg, ok := runtime.Config["configurable"].(map[string]any); ok {
				if tid, ok2 := cfg["thread_id"]; ok2 && tid != nil {
					return fmt.Sprintf("%v", tid)
				}
			}
		}
	}
	return "default"
}

// ResolveRuntimeUserID 从 runtime 提取 user_id(对应 runtime/user_context.py::resolve_runtime_user_id)。
func ResolveRuntimeUserID(runtime *Runtime) string {
	if runtime != nil && runtime.Context != nil {
		if uid, ok := runtime.Context["user_id"].(string); ok && uid != "" {
			return uid
		}
	}
	return "default"
}

// MCPToolDescriptor 是被包装的 MCP 工具(对应 langchain BaseTool)。
type MCPToolDescriptor struct {
	Name        string
	Description string
	ServerName  string
	Connection  map[string]any
}

// SessionPoolTool 把 MCP 工具包装成复用池会话的形式(实现 capability.Tool)。
type SessionPoolTool struct {
	tool         MCPToolDescriptor
	originalName string // 去掉 server 前缀后的名字
	pool         *SessionPool
	interceptors []ToolInterceptor
	workspace    Workspace
}

// MakeSessionPoolTool 构造池会话工具包装(tools.py:412-538 的 _make_session_pool_tool)。
func MakeSessionPoolTool(tool MCPToolDescriptor, pool *SessionPool, interceptors []ToolInterceptor, ws Workspace) *SessionPoolTool {
	originalName := tool.Name
	prefix := tool.ServerName + "_"
	if strings.HasPrefix(originalName, prefix) {
		originalName = originalName[len(prefix):]
	}
	return &SessionPoolTool{
		tool:         tool,
		originalName: originalName,
		pool:         pool,
		interceptors: interceptors,
		workspace:    ws,
	}
}

func (t *SessionPoolTool) Name() string        { return t.tool.Name }
func (t *SessionPoolTool) Description() string { return t.tool.Description }

// Run 满足 capability.Tool;从默认 runtime 调用。带 runtime 的完整调用用 Call。
func (t *SessionPoolTool) Run(ctx context.Context, argsJSON string) (string, error) {
	args := map[string]any{}
	if argsJSON != "" {
		if err := json.Unmarshal([]byte(argsJSON), &args); err != nil {
			return "", err
		}
	}
	res, err := t.Call(ctx, nil, args)
	if err != nil {
		return "", err
	}
	return resultToText(res), nil
}

// Call 带运行时上下文调用工具,复用池会话(tools.py:436-529 的 call_with_persistent_session)。
func (t *SessionPoolTool) Call(ctx context.Context, runtime *Runtime, args map[string]any) (*CallToolResult, error) {
	threadID := ExtractThreadID(runtime)
	userID := ResolveRuntimeUserID(runtime)
	scopeKey := ScopeKey(userID, threadID)

	sessionConnection := map[string]any{}
	for k, v := range t.tool.Connection {
		sessionConnection[k] = v
	}

	// cwd/temp 钉住 + 工作区快照只对 stdio(本地子进程)有意义;SSE/HTTP 无本地 cwd。
	isStdio := fmt.Sprintf("%v", sessionConnection["transport"]) == "stdio"
	var sourceBaseDir string
	if isStdio && t.workspace != nil {
		sourceBaseDir = t.workspace.WorkDir(threadID, userID)
		// stdio 子进程 cwd 钉在 thread 的 user-data 树内,相对输出链接才能被解析。
		configuredCWD := fmt.Sprintf("%v", sessionConnection["cwd"])
		if configuredCWD == "<nil>" || configuredCWD == "" {
			configuredCWD = sourceBaseDir
		}
		sessionConnection["cwd"] = configuredCWD
		// 把子进程 temp dir 也钉在同一挂载树内(os.tmpdir()/tempfile 产物落入 user-data)。
		tmpDir := sourceBaseDir + "/" + mcpTmpSubdir
		sessionEnv := map[string]string{}
		if env, ok := sessionConnection["env"].(map[string]string); ok {
			for k, v := range env {
				sessionEnv[k] = v
			}
		}
		if _, ok := sessionEnv["TMPDIR"]; !ok {
			sessionEnv["TMPDIR"] = tmpDir
		}
		if _, ok := sessionEnv["TMP"]; !ok {
			sessionEnv["TMP"] = tmpDir
		}
		if _, ok := sessionEnv["TEMP"]; !ok {
			sessionEnv["TEMP"] = tmpDir
		}
		sessionConnection["env"] = sessionEnv
	}

	session, err := t.pool.GetSession(ctx, t.tool.ServerName, scopeKey, sessionConnection)
	if err != nil {
		return nil, err
	}

	if len(t.interceptors) > 0 {
		baseHandler := func(req ToolCallRequest) (*CallToolResult, error) {
			var callOpts map[string]any
			if len(req.Headers) > 0 {
				callOpts = map[string]any{"meta": map[string]any{"headers": req.Headers}}
			}
			return session.CallTool(ctx, req.Name, req.Args, callOpts)
		}
		handler := baseHandler
		for i := len(t.interceptors) - 1; i >= 0; i-- {
			outer := handler
			interceptor := t.interceptors[i]
			handler = func(req ToolCallRequest) (*CallToolResult, error) {
				return interceptor(req, outer)
			}
		}
		req := ToolCallRequest{Name: t.originalName, Args: args, ServerName: t.tool.ServerName}
		return handler(req)
	}

	res, err := session.CallTool(ctx, t.originalName, args, nil)
	if err != nil {
		return nil, err
	}
	// stdio 结果里的本地文件引用改写为虚拟路径(Playwright 截图等)。
	if isStdio && t.workspace != nil && res != nil {
		rewriteResultPaths(res, threadID, userID, sourceBaseDir, t.workspace)
	}
	return res, nil
}

// resultToText 把 CallToolResult 压成文本(供 capability.Tool.Run 返回)。
func resultToText(res *CallToolResult) string {
	if res == nil {
		return ""
	}
	var parts []string
	for _, b := range res.Content {
		if b.Type == "text" {
			parts = append(parts, b.Text)
		}
	}
	return strings.Join(parts, "\n")
}

// rewriteResultPaths 改写结果里的本地文件引用(tools.py:239-288 的 _rewrite_local_paths_in_text)。
func rewriteResultPaths(res *CallToolResult, threadID, userID, sourceBaseDir string, ws Workspace) {
	for i := range res.Content {
		if res.Content[i].Type == "text" {
			res.Content[i].Text = rewriteLocalPathsInText(res.Content[i].Text, threadID, userID, sourceBaseDir, ws)
		}
	}
}

// 匹配 free text 里的本地文件引用(tools.py:42 的 _LOCAL_PATH_IN_TEXT_RE)。
var localPathInTextRe = regexp.MustCompile(`(?:file://)?/[^\s'\"<>|*?]+|(?:\.{0,2}/|[\w.-]+/)[^\s'\"<>|*?]+`)

// textPathTrailingChars 是标点/标记而非路径一部分的尾字符(tools.py:45)。
const textPathTrailingChars = ".,;:!?)]}>\"'`"

// localPathFromURI 把 uri 解析成本地绝对路径(tools.py:50-74 的 _local_path_from_uri)。
// 远程 URI(http/https/data/...)返回空串。相对路径仅在给出 baseDir 时解析。
func localPathFromURI(uri, baseDir string) string {
	if uri == "" {
		return ""
	}
	parsed, err := url.Parse(uri)
	if err != nil {
		return ""
	}
	var raw string
	switch parsed.Scheme {
	case "file":
		raw, _ = url.PathUnescape(parsed.Path)
	case "":
		raw = uri
	default:
		return ""
	}
	if raw == "" {
		return ""
	}
	if !strings.HasPrefix(raw, "/") {
		if baseDir == "" {
			return ""
		}
		raw = strings.TrimSuffix(baseDir, "/") + "/" + raw
	}
	return raw
}

// rewriteLocalPathsInText 尽力改写 free text 里的本地文件引用(tools.py:239-288)。
// 每个候选 token 交给 ws.VirtualPath,只在「能映射到 thread user-data 树内存在的文件」
// 时才改写;映射失败原样保留(过度匹配无害)。
func rewriteLocalPathsInText(text, threadID, userID, sourceBaseDir string, ws Workspace) string {
	if ws == nil {
		return text
	}
	translated := map[string]string{}

	rewritten := localPathInTextRe.ReplaceAllStringFunc(text, func(token string) string {
		stripped := strings.TrimRight(token, textPathTrailingChars)
		trailing := token[len(stripped):]
		vp, ok := translated[stripped]
		if !ok {
			vp = ""
			if local := localPathFromURI(stripped, sourceBaseDir); local != "" {
				if v, ok2 := ws.VirtualPath(local, threadID, userID); ok2 {
					vp = v
				}
			}
			translated[stripped] = vp
		}
		if vp == "" {
			return token
		}
		return vp + trailing
	})

	return rewritten
}

// ---------------------------------------------------------------------------
// tools.py:get_mcp_tools 的「按 transport 决定是否池化」编排
// ---------------------------------------------------------------------------

// WrapToolsForPooling 复现 get_mcp_tools(tools.py:627-643)的包装决策:
// 按 server 名前缀归属工具,只有 stdio transport 才套会话池包装,SSE/HTTP 原样返回。
func WrapToolsForPooling(tools []MCPToolDescriptor, serversConfig map[string]map[string]any, pool *SessionPool, interceptors []ToolInterceptor, ws Workspace) []capability.Tool {
	out := make([]capability.Tool, 0, len(tools))
	for _, tool := range tools {
		server := serverForTool(tool.Name, serversConfig)
		if server == "" {
			out = append(out, staticTool{tool})
			continue
		}
		transport, _ := serversConfig[server]["transport"].(string)
		if transport == "" {
			transport = "stdio"
		}
		if transport == "stdio" {
			out = append(out, MakeSessionPoolTool(tool, pool, interceptors, ws))
		} else {
			out = append(out, staticTool{tool})
		}
	}
	return out
}

// serverForTool 按名字前缀匹配 server(tools.py:629-633 的 tool_server 归属)。
func serverForTool(toolName string, serversConfig map[string]map[string]any) string {
	for name := range serversConfig {
		if strings.HasPrefix(toolName, name+"_") {
			return name
		}
	}
	return ""
}

// staticTool 是不池化的 MCP 工具(SSE/HTTP transport 或无法归属的),直接透传调用。
type staticTool struct {
	desc MCPToolDescriptor
}

func (s staticTool) Name() string        { return s.desc.Name }
func (s staticTool) Description() string { return s.desc.Description }
func (s staticTool) Run(_ context.Context, argsJSON string) (string, error) {
	return "", errors.New("static MCP tool is a skeleton; bind a real MCP client to invoke it")
}
