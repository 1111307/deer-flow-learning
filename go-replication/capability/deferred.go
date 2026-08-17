// 延迟工具绑定(Deferred Tool)—— 对应 deer-flow 两个源文件:
//
//   - tools/builtins/tool_search.py(DeferredToolCatalog / build_tool_search_tool /
//     build_deferred_tool_setup / assemble_deferred_tools / get_deferred_tools_prompt_section)
//   - agents/middlewares/deferred_tool_filter_middleware.py(DeferredToolFilterMiddleware)
//
// 核心设计:
//
//	接入多个 MCP server 后工具几十上百个,全部塞进 model 的 tool schema 会占满 context、
//	降低选工具准确率。解法是「动态检索 + 提升」两阶段:
//	  1. 目录阶段:模型只看到名字(渲染成 <available-deferred-tools>),schema 被扣住。
//	  2. 提升阶段:模型通过 tool_search 按需换取完整 schema,命中后才真正可调用。
//	提升记录用 catalog_hash 打版本戳:目录一变 hash 就变,旧提升名单整体失效,
//	防止「名字复用指向了不同工具」。
//
// 与 Python 的关键差异:
//   - deer-flow 的 tool_search 返回 langgraph.types.Command(update={"promoted":..., "messages":[...]})
//     把「提升」写进 graph state。Go 的 Tool.Run 只返回 (string, error),所以这里:
//   - Run 返回模型可见的 schema JSON(对应 Command.update["messages"]);
//   - tool_search 工具额外实现 Promoter 接口,把「提升名单 + 目录哈希」暴露给 harness
//     (对应 Command.update["promoted"]),由 harness 合并进 ThreadState。
//   - deer-flow 用 metadata["deerflow_mcp"] 标记 MCP 工具;Go 里 Tool 接口没有 metadata,
//     用可选标记接口 MCPTool(IsMCPTool() bool) 表达同一件事。
//   - re 正则退化为 regexp(RE2):不支持 lookahead/lookbehind,但「编译失败降级字面匹配」
//     的语义完全一致(regexp.QuoteMeta 对应 re.escape)。
//   - frozenset 退化为 map[string]bool / []string(排序后)。
package capability

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
)

// MaxDeferredResults 每次 tool_search 最多返回的工具数(对应 MAX_RESULTS=5)。
const MaxDeferredResults = 5

// MCPTool 标记一个工具来自 MCP server(对应 deer-flow 的 metadata["deerflow_mcp"]==True)。
// 只有 MCP 工具会被「延迟」:目录只给名字,模型经 tool_search 提升后才拿到完整 schema。
// 任何实现 Tool 的类型只要再加一个 IsMCPTool() bool 方法即可被识别。
type MCPTool interface {
	Tool
	IsMCPTool() bool
}

// IsMCPTool 判断工具是否携带 MCP 标记(对应 mcp_metadata.py::is_mcp_tool)。
func IsMCPTool(t Tool) bool {
	if m, ok := t.(MCPTool); ok {
		return m.IsMCPTool()
	}
	return false
}

// toolSchemaMap 返回工具的完整 schema(对应 convert_to_openai_function 的 name/description
// 部分;Tool 接口没有 parameters,故 schema 只含 name + description)。
func toolSchemaMap(t Tool) map[string]string {
	return map[string]string{
		"name":        t.Name(),
		"description": t.Description(),
	}
}

// ToolSchema 返回工具的完整 schema(JSON 字符串)。tool_search 把命中工具的 schema 返回给模型。
func ToolSchema(t Tool) string {
	b, _ := json.Marshal(toolSchemaMap(t))
	return string(b)
}

// DeferredCatalog 是不可变的延迟工具目录,只做纯搜索,不持有可变状态。
// 对应 tool_search.py::DeferredToolCatalog(@dataclass(frozen=True))。
type DeferredCatalog struct {
	tools []Tool // 构造时做防御性拷贝,构造后不再修改(等价 frozen + tuple)
	names []string
	hash  string
}

// NewDeferredCatalog 从工具列表构造不可变目录。工具切片做防御性拷贝,
// names/hash 在构造时一次性算好(等价 deer-flow 的 @cached_property,但更简单且线程安全)。
func NewDeferredCatalog(tools []Tool) *DeferredCatalog {
	c := &DeferredCatalog{
		tools: make([]Tool, len(tools)),
	}
	copy(c.tools, tools)

	// names:frozenset 退化为排序切片,既稳定又便于渲染。
	names := make([]string, 0, len(c.tools))
	for _, t := range c.tools {
		names = append(names, t.Name())
	}
	sort.Strings(names)
	c.names = names

	c.hash = computeCatalogHash(c.tools)
	return c
}

// computeCatalogHash 复现 DeferredToolCatalog.hash:
//
//	按名字排序 -> [{"name", "schema"}] -> json.dumps(sort_keys=True) -> sha256 -> 前 16 位。
//
// Go 的 json.Marshal 对 map key 天然排序,等效 sort_keys=True。
func computeCatalogHash(tools []Tool) string {
	type entry struct {
		Name   string            `json:"name"`
		Schema map[string]string `json:"schema"`
	}
	sorted := make([]Tool, len(tools))
	copy(sorted, tools)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name() < sorted[j].Name() })

	canon := make([]entry, 0, len(sorted))
	for _, t := range sorted {
		canon = append(canon, entry{Name: t.Name(), Schema: toolSchemaMap(t)})
	}
	blob, err := json.Marshal(canon)
	if err != nil {
		// 不会发生:name/description 都是 string,一定可序列化。
		blob = []byte{}
	}
	sum := sha256.Sum256(blob)
	return hex.EncodeToString(sum[:])[:16]
}

// Names 返回排序后的目录名(渲染成 <available-deferred-tools> 给模型看)。
func (c *DeferredCatalog) Names() []string {
	return append([]string(nil), c.names...)
}

// NamesSet 返回目录名的集合(frozenset 的等价物),用于 O(1) 归属判断。
func (c *DeferredCatalog) NamesSet() map[string]bool {
	set := make(map[string]bool, len(c.names))
	for _, n := range c.names {
		set[n] = true
	}
	return set
}

// Len 返回目录里的工具数量。
func (c *DeferredCatalog) Len() int {
	return len(c.tools)
}

// Hash 返回目录哈希(目录变了 hash 变,用于 promote 的 catalog_hash 分区)。
func (c *DeferredCatalog) Hash() string {
	return c.hash
}

// compileCatalogRegex 编译查询正则,失败时降级为字面匹配。
// 对应 _compile_catalog_regex:查询来自模型,非法正则(如不配对括号)必须降级而非报错。
func compileCatalogRegex(pattern string) *regexp.Regexp {
	if re, err := regexp.Compile("(?i)" + pattern); err == nil {
		return re
	}
	return regexp.MustCompile("(?i)" + regexp.QuoteMeta(pattern))
}

// catalogRegexScore 返回正则 pattern 在 "name description" 上的匹配次数。
// 对应 _catalog_regex_score(用于 "+token 剩余词" 的排序打分)。
func catalogRegexScore(pattern string, t Tool) int {
	re := compileCatalogRegex(pattern)
	return len(re.FindAllString(t.Name()+" "+t.Description(), -1))
}

// Search 按查询匹配工具。对应 DeferredCatalog.search 的三种查询形式:
//   - "select:A,B" 精确按名字选
//   - "+slack send"  名字必须含某 token,再按剩余词排序
//   - 其它           正则/关键字匹配(名字命中权重 2,描述命中权重 1)
//
// 三种形式都截断到 MAX_RESULTS。
func (c *DeferredCatalog) Search(query string) []Tool {
	query = strings.TrimSpace(query)
	if query == "" {
		return nil
	}

	// 形式 1:select: 精确按名字选。
	if strings.HasPrefix(query, "select:") {
		wanted := make(map[string]bool)
		for _, n := range strings.Split(query[len("select:"):], ",") {
			if s := strings.TrimSpace(n); s != "" {
				wanted[s] = true
			}
		}
		var out []Tool
		for _, t := range c.tools {
			if wanted[t.Name()] {
				out = append(out, t)
			}
		}
		return limitDeferred(out)
	}

	// 形式 2:"+" 前缀:名字必须含 required token。
	if strings.HasPrefix(query, "+") {
		rest := strings.TrimPrefix(query, "+")
		fields := strings.Fields(rest)
		if len(fields) == 0 {
			return nil // 裸 "+" 没有必须 token —— 无事可要求。
		}
		required := strings.ToLower(fields[0])
		var candidates []Tool
		for _, t := range c.tools {
			if strings.Contains(strings.ToLower(t.Name()), required) {
				candidates = append(candidates, t)
			}
		}
		if len(fields) > 1 {
			extra := strings.Join(fields[1:], " ")
			// 对应 split(None, 1):剩余词作为一个整体参与排序。
			sort.SliceStable(candidates, func(i, j int) bool {
				return catalogRegexScore(extra, candidates[i]) > catalogRegexScore(extra, candidates[j])
			})
		}
		return limitDeferred(candidates)
	}

	// 形式 3:正则/关键字匹配(名字命中权重 2,描述命中权重 1)。
	re := compileCatalogRegex(query)
	var best []Tool // 名字命中,权重 2
	var rest []Tool // 描述命中,权重 1
	for _, t := range c.tools {
		searchable := t.Name() + " " + t.Description()
		if !re.MatchString(searchable) {
			continue
		}
		if re.MatchString(t.Name()) {
			best = append(best, t)
		} else {
			rest = append(rest, t)
		}
	}
	return limitDeferred(append(best, rest...))
}

// limitDeferred 截断到 MaxDeferredResults。
func limitDeferred(tools []Tool) []Tool {
	if len(tools) > MaxDeferredResults {
		return tools[:MaxDeferredResults]
	}
	return tools
}

// Promoted 是 graph state 中 "promoted" 字段的载荷:目录哈希 + 提升的工具名。
// 对应 thread_state.py::PromotedTools。capability 不 import harness,这里独立声明;
// harness 侧的 PromotedTools(见 harness/state.go)与其语义一致,由 harness 做适配。
type Promoted struct {
	CatalogHash string
	Names       []string
}

// Promoter 是「返回 Command(update={"promoted":...})」的工具在 Go 里的形态:
//   - Run 返回模型可见的字符串(schema JSON);
//   - Promoted 返回本次调用要合并进 graph state 的提升记录。
//
// harness 在跑完一个工具后做类型断言:若命中 Promoter,就把 Promoted() 合并进
// ThreadState.promoted(用 harness.MergePromoted 按 catalog_hash 分区)。
type Promoter interface {
	Tool
	Promoted() *Promoted
}

// toolSearchTool 是 tool_search 工具(对应 build_tool_search_tool 的闭包产物)。
// 它捕获目录与 catalog_hash(对应 deer-flow 里 build-time 捕获的 catalog_hash)。
type toolSearchTool struct {
	catalog     *DeferredCatalog
	catalogHash string

	mu   sync.Mutex
	last *Promoted // 最近一次 Run 产生的提升记录(线程安全)
}

func (t *toolSearchTool) Name() string { return "tool_search" }

func (t *toolSearchTool) Description() string {
	return "Fetches full schema definitions for deferred tools so they can be called. " +
		"Query forms: \"select:Read,Edit\" for exact names, \"+slack send\" to require a token, " +
		"or a keyword/regex query. Matched tools become callable after this returns."
}

func (t *toolSearchTool) Run(_ context.Context, argsJSON string) (string, error) {
	var args struct {
		Query string `json:"query"`
	}
	if err := json.Unmarshal([]byte(argsJSON), &args); err != nil {
		return "", err
	}

	matched := t.catalog.Search(args.Query)
	var content string
	var names []string
	if len(matched) == 0 {
		content = "No tools found matching: " + args.Query
	} else {
		schemas := make([]map[string]string, 0, len(matched))
		for _, m := range matched {
			schemas = append(schemas, toolSchemaMap(m))
			names = append(names, m.Name())
		}
		b, err := json.MarshalIndent(schemas, "", "  ")
		if err != nil {
			return "", err
		}
		content = string(b)
	}

	// 对应 Command.update["promoted"] = {"catalog_hash": catalog_hash, "names": names}。
	t.mu.Lock()
	t.last = &Promoted{CatalogHash: t.catalogHash, Names: names}
	t.mu.Unlock()

	return content, nil
}

// Promoted 返回最近一次提升记录(实现 Promoter 接口)。
func (t *toolSearchTool) Promoted() *Promoted {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.last
}

// buildToolSearchTool 构造 tool_search 工具(对应 build_tool_search_tool)。
func buildToolSearchTool(catalog *DeferredCatalog) Tool {
	return &toolSearchTool{catalog: catalog, catalogHash: catalog.Hash()}
}

// DeferredToolSetup 是「为一个 agent build 组装延迟工具」的结果(对应 DeferredToolSetup)。
// 三个字段作为一个整体移动,调用方只需看 ToolSearchTool 是否为 nil:
//   - 空(nil + 空名字 + 空 hash):未启用延迟,或没有 MCP 工具通过策略过滤。
//   - 非空:ToolSearchTool 被追加到工具列表,DeferredNames 在提升前对模型扣住,
//     CatalogHash 在 graph state 里给提升名单打版本戳。
//
// 不变量:ToolSearchTool == nil ⟺ DeferredNames 空 ⟺ CatalogHash 空。
type DeferredToolSetup struct {
	ToolSearchTool Tool
	DeferredNames  []string
	CatalogHash    string
}

// Empty 报告该 setup 是否为空(未启用 / 无 MCP 工具)。
func (s *DeferredToolSetup) Empty() bool {
	return s.ToolSearchTool == nil && len(s.DeferredNames) == 0 && s.CatalogHash == ""
}

// emptyDeferredSetup 返回一个空的 setup。
func emptyDeferredSetup() *DeferredToolSetup {
	return &DeferredToolSetup{}
}

// BuildDeferredToolSetup 从「策略过滤后」的工具列表构建延迟工具 setup。
// 对应 build_deferred_tool_setup:必须在 skill/agent 工具策略过滤之后调用,
// 目录才永远不会暴露当前 agent 无权使用的工具。
//
// 返回空 setup 的两种情况(deer-flow 里明确区分):
//   - 未启用:绑定所有工具 schema,与旧行为一致。
//   - 已启用但没有 MCP 工具通过过滤:同样为空,但原因不同。
func BuildDeferredToolSetup(filteredTools []Tool, enabled bool) *DeferredToolSetup {
	if !enabled {
		return emptyDeferredSetup()
	}
	var deferred []Tool
	for _, t := range filteredTools {
		if IsMCPTool(t) {
			deferred = append(deferred, t)
		}
	}
	if len(deferred) == 0 {
		return emptyDeferredSetup()
	}
	catalog := NewDeferredCatalog(deferred)
	return &DeferredToolSetup{
		ToolSearchTool: buildToolSearchTool(catalog),
		DeferredNames:  catalog.Names(),
		CatalogHash:    catalog.Hash(),
	}
}

// AssembleDeferredTools 构建最终工具列表 + 延迟 setup(对应 assemble_deferred_tools)。
//
// 必须在策略过滤之后调用。fail-closed:如果 tool_search 已启用、且 MCP 工具通过过滤,
// 但没有拿到延迟 set,则报错 —— 而不是静默地把完整 MCP schema 绑定给模型。
// 所有 agent 构建路径(lead / embedded / subagent)共用这一处,保证同样的 fail-closed 语义。
func AssembleDeferredTools(filteredTools []Tool, enabled bool) ([]Tool, *DeferredToolSetup, error) {
	setup := BuildDeferredToolSetup(filteredTools, enabled)
	if enabled && len(setup.DeferredNames) == 0 && anyMCPTool(filteredTools) {
		return nil, nil, fmt.Errorf("tool_search enabled and MCP tools survived policy filtering, but no deferred set was recovered - refusing to bind MCP schemas (fail-closed)")
	}
	final := make([]Tool, len(filteredTools))
	copy(final, filteredTools)
	if setup.ToolSearchTool != nil {
		final = append(final, setup.ToolSearchTool)
	}
	return final, setup, nil
}

// anyMCPTool 判断列表里是否有任何 MCP 工具(对应 any(is_mcp_tool(t) for t in ...))。
func anyMCPTool(tools []Tool) bool {
	for _, t := range tools {
		if IsMCPTool(t) {
			return true
		}
	}
	return false
}

// GetDeferredToolsPromptSection 生成 <available-deferred-tools> 段(对应同名函数)。
// 只列名字,让 agent 知道存在哪些工具、并可用 tool_search 加载它们;无延迟工具时返回空串。
func GetDeferredToolsPromptSection(deferredNames []string) string {
	if len(deferredNames) == 0 {
		return ""
	}
	sorted := append([]string(nil), deferredNames...)
	sort.Strings(sorted)
	return "<available-deferred-tools>\n" + strings.Join(sorted, "\n") + "\n</available-deferred-tools>"
}

// DeferredToolFilterMiddleware 隐藏尚未提升的延迟工具 schema。
// 对应 deferred_tool_filter_middleware.py::DeferredToolFilterMiddleware。
//
// ToolNode 仍持有全部工具(含延迟工具)用于执行路由,但模型只能看到「活跃工具 schema +
// 已提升的延迟工具 schema」。提升状态从 graph state 的 promoted 字段读取,并按
// catalog_hash 分区 —— 持久化的旧提升记录不能暴露一个改名/漂移后的工具。
type DeferredToolFilterMiddleware struct {
	deferred    map[string]bool
	catalogHash string
}

// NewDeferredToolFilterMiddleware 构造中间件。deferredNames 是延迟工具名集,
// catalogHash 是当前目录哈希(与 DeferredToolSetup.CatalogHash 一致)。
func NewDeferredToolFilterMiddleware(deferredNames []string, catalogHash string) *DeferredToolFilterMiddleware {
	m := &DeferredToolFilterMiddleware{
		deferred:    make(map[string]bool, len(deferredNames)),
		catalogHash: catalogHash,
	}
	for _, n := range deferredNames {
		m.deferred[n] = true
	}
	return m
}

// Deferred 返回延迟工具名集(只读快照,供观测/测试)。
func (m *DeferredToolFilterMiddleware) Deferred() map[string]bool {
	out := make(map[string]bool, len(m.deferred))
	for k, v := range m.deferred {
		out[k] = v
	}
	return out
}

// promotedNames 返回当前目录哈希下的提升名单;哈希不匹配视为无提升。
// 对应 _promoted(state):promoted 且 catalog_hash 匹配才生效。
func (m *DeferredToolFilterMiddleware) promotedNames(promoted *Promoted) map[string]bool {
	if promoted != nil && promoted.CatalogHash == m.catalogHash {
		return stringSet(promoted.Names)
	}
	return map[string]bool{}
}

// Hidden 返回「延迟但尚未提升」的工具名集(对应 _hidden(state) = deferred - promoted)。
func (m *DeferredToolFilterMiddleware) Hidden(promoted *Promoted) map[string]bool {
	if len(m.deferred) == 0 {
		return nil
	}
	prom := m.promotedNames(promoted)
	out := make(map[string]bool, len(m.deferred))
	for n := range m.deferred {
		if !prom[n] {
			out[n] = true
		}
	}
	return out
}

// FilterTools 从「绑定给模型」的工具列表里移除尚未提升的延迟工具 schema。
// 对应 wrap_model_call -> _filter_tools:request.tools 过滤掉 hide 集合。
func (m *DeferredToolFilterMiddleware) FilterTools(tools []Tool, promoted *Promoted) []Tool {
	if len(m.deferred) == 0 {
		return tools
	}
	hide := m.Hidden(promoted)
	if len(hide) == 0 {
		return tools
	}
	out := make([]Tool, 0, len(tools))
	for _, t := range tools {
		if !hide[t.Name()] {
			out = append(out, t)
		}
	}
	return out
}

// BlockedToolMessage 若工具尚未提升,返回一条 error 消息与 true;否则返回 "", false。
// 对应 wrap_tool_call -> _blocked_tool_message:拦截对未提升工具的调用,让模型
// 「先调 tool_search 提升,再重试」,而不是把调用直接路由进 ToolNode。
func (m *DeferredToolFilterMiddleware) BlockedToolMessage(name string, promoted *Promoted) (string, bool) {
	if len(m.deferred) == 0 {
		return "", false
	}
	if name == "" {
		return "", false
	}
	hide := m.Hidden(promoted)
	if !hide[name] {
		return "", false
	}
	return fmt.Sprintf("Error: Tool '%s' is deferred and has not been promoted yet. Call tool_search first to expose and promote this tool's schema, then retry.", name), true
}

// stringSet 把切片转成集合。
func stringSet(names []string) map[string]bool {
	out := make(map[string]bool, len(names))
	for _, n := range names {
		out[n] = true
	}
	return out
}
