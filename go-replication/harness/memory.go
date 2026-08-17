// 长期记忆(防抖队列 + 原子写 + LLM 更新)—— 对应 deer-flow 四个源文件:
//
//   - agents/memory/queue.py::MemoryUpdateQueue(防抖合并队列)
//   - agents/memory/storage.py::FileMemoryStorage(mtime 缓存 + temp+replace 原子写 + 隔离)
//   - agents/memory/updater.py::MemoryUpdater(LLM 抽取事实 + 归一化 + 应用更新)
//   - agents/memory/message_processing.py(过滤消息 + 修正/强化信号检测)
//   - 以及 prompt.py 里的 MEMORY_UPDATE_PROMPT / format_conversation_for_update
//
// 三个必考工程细节(和 deer-flow 一致):
//  1. 防抖合并:同一 (thread, user, agent) 在防抖窗口内的多次更新合并成一次处理,
//     省 LLM 调用、不阻塞主响应路径。合并时 correction/reinforcement 信号用 OR
//     语义(窗口内「曾经」出现过的信号不丢)。
//  2. 身份捕获:user_id 必须在入队时显式捕获存进结构体,不能依赖跨 goroutine 的
//     上下文传递 —— Python 的 ContextVar 不跨线程传播,Go 的 context.Context 同样
//     不自动携带业务身份跨 goroutine。
//  3. 原子写:先写同目录唯一临时文件再 rename,避免读到写一半的 JSON。
//
// 与 Python 的关键差异:
//   - threading.Timer + threading.Lock -> time.AfterFunc + sync.Mutex。
//   - uuid.uuid4().hex[:8] -> crypto/rand 随机 hex。
//   - dict[str, Any] -> MemoryData(map[string]any);re 正则退化到 regexp(RE2,无 lookaround)。
//   - str.casefold() -> strings.ToLower(ASCII 等价;对非 ASCII 略有差异)。
//   - asyncio.get_running_loop() 的「在事件循环内则 offload 到线程池」这一分支在 Go
//     里不复存在:goroutine 没有事件循环,同步调用天然不阻塞其它 goroutine。
//   - LLM 调用通过 MemoryModel 接口注入(对应 model.invoke),不 import 具体供应商。
package harness

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"deerflow-go/capability"
)

// ── 配置 ─────────────────────────────────────────────────────────────────────

// MemoryConfig 对应 memory_config.py::MemoryConfig(记忆机制配置)。
type MemoryConfig struct {
	Enabled                 bool    // 默认 true
	StoragePath             string  // 默认 ""(空 = 按 user/agent 隔离)
	StorageClass            string  // 默认 FileMemoryStorage
	DebounceSeconds         int     // 默认 30(ge=1, le=300)
	ModelName               string  // 默认 ""(用默认模型)
	MaxFacts                int     // 默认 100(ge=10, le=500)
	FactConfidenceThreshold float64 // 默认 0.7
	BaseDir                 string  // 应用数据根目录(对应 Paths.base_dir)
}

// DefaultMemoryConfig 返回与 deer-flow 默认值一致的配置。
func DefaultMemoryConfig() MemoryConfig {
	return MemoryConfig{
		Enabled:                 true,
		StorageClass:            "deerflow.agents.memory.storage.FileMemoryStorage",
		DebounceSeconds:         30,
		MaxFacts:                100,
		FactConfidenceThreshold: 0.7,
		BaseDir:                 defaultBaseDir(),
	}
}

// defaultBaseDir 对应 Paths.base_dir 的解析:DEER_FLOW_HOME 环境变量优先,
// 否则落到项目根的 .deer-flow 目录。
func defaultBaseDir() string {
	if env := os.Getenv("DEER_FLOW_HOME"); env != "" {
		return env
	}
	return filepath.Join(".", ".deer-flow")
}

// ── 记忆数据结构与时间戳 ─────────────────────────────────────────────────────

// MemoryData 是记忆载荷(对应 storage.py 的 dict[str, Any])。
type MemoryData map[string]any

// CreateEmptyMemory 创建空记忆结构(对应 create_empty_memory)。
func CreateEmptyMemory() MemoryData {
	return MemoryData{
		"version":     "1.0",
		"lastUpdated": utcNowISOZ(),
		"user": map[string]any{
			"workContext":     map[string]any{"summary": "", "updatedAt": ""},
			"personalContext": map[string]any{"summary": "", "updatedAt": ""},
			"topOfMind":       map[string]any{"summary": "", "updatedAt": ""},
		},
		"history": map[string]any{
			"recentMonths":       map[string]any{"summary": "", "updatedAt": ""},
			"earlierContext":     map[string]any{"summary": "", "updatedAt": ""},
			"longTermBackground": map[string]any{"summary": "", "updatedAt": ""},
		},
		"facts": []any{},
	}
}

// utcNowISOZ 返回带 Z 后缀的 UTC ISO-8601 时间(对应 utc_now_iso_z)。
func utcNowISOZ() string {
	return time.Now().UTC().Format("2006-01-02T15:04:05.000000Z")
}

// currentFacts 返回记忆里的 facts 切片(不存在返回 nil)。
func currentFacts(m MemoryData) []any {
	if f, ok := m["facts"].([]any); ok {
		return f
	}
	return nil
}

// ── 存储 ─────────────────────────────────────────────────────────────────────

// MemoryStorage 是记忆存储的抽象契约(对应 storage.py::MemoryStorage ABC)。
// 空字符串 agentName / userID 表示「无」,等价 Python 的 None(全局记忆)。
type MemoryStorage interface {
	Load(agentName, userID string) (MemoryData, error)
	Reload(agentName, userID string) (MemoryData, error)
	Save(data MemoryData, agentName, userID string) (bool, error)
}

// agentNamePattern 对应 AGENT_NAME_PATTERN = ^[A-Za-z0-9-]+$(防路径穿越)。
var agentNamePattern = regexp.MustCompile(`^[A-Za-z0-9-]+$`)

// userIDPattern 对应 paths.py 的 _SAFE_USER_ID_RE = ^[A-Za-z0-9_\-]+$。
var userIDPattern = regexp.MustCompile(`^[A-Za-z0-9_\-]+$`)

func validateAgentName(name string) error {
	if name == "" {
		return fmt.Errorf("agent name must be a non-empty string")
	}
	if !agentNamePattern.MatchString(name) {
		return fmt.Errorf("invalid agent name %q: names must match %s", name, agentNamePattern.String())
	}
	return nil
}

func validateUserID(id string) error {
	if !userIDPattern.MatchString(id) {
		return fmt.Errorf("invalid user_id %q: only alphanumeric characters, hyphens, and underscores are allowed", id)
	}
	return nil
}

// memoryCacheEntry 是缓存条目:(data, file_mtime)。mtime 为 nil 表示文件不存在。
type memoryCacheEntry struct {
	data  MemoryData
	mtime *float64
}

// FileMemoryStorage 是文件型记忆存储(对应 FileMemoryStorage)。
// 缓存按 (user_id, agent_name) 隔离,读写由同一把锁保护。
type FileMemoryStorage struct {
	mu      sync.Mutex
	cache   map[string]*memoryCacheEntry
	baseDir string
	config  MemoryConfig
}

// NewFileMemoryStorage 构造文件存储。
func NewFileMemoryStorage(cfg MemoryConfig) *FileMemoryStorage {
	if cfg.BaseDir == "" {
		cfg.BaseDir = defaultBaseDir()
	}
	return &FileMemoryStorage{
		cache:   map[string]*memoryCacheEntry{},
		baseDir: cfg.BaseDir,
		config:  cfg,
	}
}

// cacheKey 对应 _cache_key:返回 (user_id, agent_name) 的稳定编码。
func memoryCacheKey(agentName, userID string) string {
	return userID + "\x00" + agentName
}

// memoryFilePath 对应 _get_memory_file_path:per-user/agent 隔离 + storage_path 覆盖。
// 返回错误用于校验非法 agent/user 名(对应 ValueError)。
func (s *FileMemoryStorage) memoryFilePath(agentName, userID string) (string, error) {
	if userID != "" {
		if agentName != "" {
			if err := validateAgentName(agentName); err != nil {
				return "", err
			}
			return filepath.Join(s.baseDir, "users", userID, "agents", strings.ToLower(agentName), "memory.json"), nil
		}
		if s.config.StoragePath != "" && filepath.IsAbs(s.config.StoragePath) {
			// 绝对路径照用,并显式退出 per-user 隔离(所有用户共享同一文件)。
			return s.config.StoragePath, nil
		}
		if err := validateUserID(userID); err != nil {
			return "", err
		}
		return filepath.Join(s.baseDir, "users", userID, "memory.json"), nil
	}

	// 遗留:无 user_id。
	if agentName != "" {
		if err := validateAgentName(agentName); err != nil {
			return "", err
		}
		return filepath.Join(s.baseDir, "agents", strings.ToLower(agentName), "memory.json"), nil
	}
	if s.config.StoragePath != "" {
		p := s.config.StoragePath
		if !filepath.IsAbs(p) {
			p = filepath.Join(s.baseDir, p)
		}
		return p, nil
	}
	return filepath.Join(s.baseDir, "memory.json"), nil
}

// loadFromFile 对应 _load_memory_from_file:文件缺失或损坏 -> 空记忆。
func (s *FileMemoryStorage) loadFromFile(agentName, userID string) MemoryData {
	filePath, err := s.memoryFilePath(agentName, userID)
	if err != nil {
		return CreateEmptyMemory()
	}
	raw, err := os.ReadFile(filePath)
	if err != nil {
		return CreateEmptyMemory()
	}
	var data MemoryData
	if err := json.Unmarshal(raw, &data); err != nil {
		return CreateEmptyMemory()
	}
	return data
}

// fileMtime 返回文件的 mtime(nil 表示不存在或 stat 失败)。对应 stat().st_mtime。
func fileMtime(path string) *float64 {
	info, err := os.Stat(path)
	if err != nil {
		return nil
	}
	m := float64(info.ModTime().UnixNano()) / 1e9
	return &m
}

func mtimeEqual(a, b *float64) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

// Load 加载记忆(mtime 缓存)。对应 FileMemoryStorage.load。
func (s *FileMemoryStorage) Load(agentName, userID string) (MemoryData, error) {
	filePath, err := s.memoryFilePath(agentName, userID)
	if err != nil {
		return nil, err
	}
	key := memoryCacheKey(agentName, userID)
	currentMtime := fileMtime(filePath)

	s.mu.Lock()
	if cached, ok := s.cache[key]; ok && mtimeEqual(cached.mtime, currentMtime) {
		s.mu.Unlock()
		return cached.data, nil
	}
	s.mu.Unlock()

	data := s.loadFromFile(agentName, userID)

	s.mu.Lock()
	s.cache[key] = &memoryCacheEntry{data: data, mtime: currentMtime}
	s.mu.Unlock()
	return data, nil
}

// Reload 强制从文件重载,失效缓存。对应 FileMemoryStorage.reload。
func (s *FileMemoryStorage) Reload(agentName, userID string) (MemoryData, error) {
	filePath, err := s.memoryFilePath(agentName, userID)
	if err != nil {
		return nil, err
	}
	key := memoryCacheKey(agentName, userID)
	data := s.loadFromFile(agentName, userID)

	s.mu.Lock()
	s.cache[key] = &memoryCacheEntry{data: data, mtime: fileMtime(filePath)}
	s.mu.Unlock()
	return data, nil
}

// Save 保存记忆到文件并更新缓存(对应 FileMemoryStorage.save)。
// 关键:浅拷贝后再加 lastUpdated,避免把时间戳写进调用方的 dict 或缓存引用;
// 用同目录唯一临时文件 + rename 实现原子替换,避免读到写一半的 JSON。
func (s *FileMemoryStorage) Save(data MemoryData, agentName, userID string) (bool, error) {
	filePath, err := s.memoryFilePath(agentName, userID)
	if err != nil {
		return false, err
	}
	key := memoryCacheKey(agentName, userID)

	if err := os.MkdirAll(filepath.Dir(filePath), 0o755); err != nil {
		return false, nil // 对应 except OSError -> False
	}

	// 浅拷贝 + lastUpdated(不 mutate 调用方)。
	copyData := make(MemoryData, len(data)+1)
	for k, v := range data {
		copyData[k] = v
	}
	copyData["lastUpdated"] = utcNowISOZ()

	blob, err := json.MarshalIndent(copyData, "", "  ")
	if err != nil {
		return false, nil
	}

	// 唯一临时文件 + 原子替换。对应 temp_path.replace(file_path)。
	tmp, err := os.CreateTemp(filepath.Dir(filePath), ".memory-*.tmp")
	if err != nil {
		return false, nil
	}
	tmpPath := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpPath) }
	if _, err := tmp.Write(blob); err != nil {
		_ = tmp.Close()
		cleanup()
		return false, nil
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return false, nil
	}
	if err := os.Rename(tmpPath, filePath); err != nil {
		cleanup()
		return false, nil
	}

	s.mu.Lock()
	s.cache[key] = &memoryCacheEntry{data: copyData, mtime: fileMtime(filePath)}
	s.mu.Unlock()
	return true, nil
}

// storageFactories 对应 storage_class 反射注册;未知 class 回退 FileMemoryStorage。
var (
	memoryStorageMu        sync.Mutex
	memoryStorageInstance  MemoryStorage
	memoryStorageFactories = map[string]func(MemoryConfig) MemoryStorage{}
)

// RegisterMemoryStorage 注册一个记忆存储工厂(对应 storage_class 的反射解析)。
func RegisterMemoryStorage(name string, f func(MemoryConfig) MemoryStorage) {
	memoryStorageMu.Lock()
	defer memoryStorageMu.Unlock()
	memoryStorageFactories[name] = f
}

// GetMemoryStorage 返回全局存储单例(对应 get_memory_storage)。
// 按 config.StorageClass 查表,失败则回退 FileMemoryStorage(对应 except -> fallback)。
func GetMemoryStorage() MemoryStorage {
	memoryStorageMu.Lock()
	defer memoryStorageMu.Unlock()
	if memoryStorageInstance == nil {
		cfg := getMemoryConfig()
		if f, ok := memoryStorageFactories[cfg.StorageClass]; ok {
			memoryStorageInstance = f(cfg)
		} else {
			memoryStorageInstance = NewFileMemoryStorage(cfg)
		}
	}
	return memoryStorageInstance
}

// SetMemoryStorage 覆盖全局存储(测试用)。
func SetMemoryStorage(s MemoryStorage) {
	memoryStorageMu.Lock()
	defer memoryStorageMu.Unlock()
	memoryStorageInstance = s
}

// ResetMemoryStorage 清空全局存储单例(测试用)。
func ResetMemoryStorage() {
	memoryStorageMu.Lock()
	defer memoryStorageMu.Unlock()
	memoryStorageInstance = nil
}

// ── 防抖队列 ─────────────────────────────────────────────────────────────────

// ConversationContext 是一次待处理的记忆更新。所有身份字段在入队时捕获。
// 对应 queue.py::ConversationContext。
type ConversationContext struct {
	ThreadID      string
	UserID        string
	AgentName     string
	Messages      []capability.Message
	Timestamp     time.Time
	Correction    bool // 修正信号(OR 合并;对应 correction_detected)
	Reinforcement bool // 强化信号(OR 合并;对应 reinforcement_detected)
}

// MemoryQueue 是带防抖的记忆更新队列。对应 MemoryUpdateQueue。
type MemoryQueue struct {
	mu         sync.Mutex
	queue      map[string]*ConversationContext
	timer      *time.Timer
	debounce   time.Duration
	enabled    bool
	process    func([]*ConversationContext) // 批量处理器(真实实现调 MemoryUpdater)
	processing bool
}

// NewMemoryQueue 构造防抖队列。debounce 是窗口时长,process 是批量处理器。
func NewMemoryQueue(debounce time.Duration, process func([]*ConversationContext)) *MemoryQueue {
	return &MemoryQueue{
		queue:    map[string]*ConversationContext{},
		debounce: debounce,
		enabled:  true,
		process:  process,
	}
}

// NewMemoryQueueWithConfig 从 MemoryConfig 构造队列(对应 get_memory_config 驱动)。
func NewMemoryQueueWithConfig(cfg MemoryConfig, process func([]*ConversationContext)) *MemoryQueue {
	q := NewMemoryQueue(time.Duration(cfg.DebounceSeconds)*time.Second, process)
	q.enabled = cfg.Enabled
	return q
}

// SetEnabled 开关队列(对应 config.enabled)。
func (q *MemoryQueue) SetEnabled(enabled bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.enabled = enabled
}

func memoryKey(threadID, userID, agentName string) string {
	return threadID + "|" + userID + "|" + agentName
}

// Add 入队并重置防抖定时器。对应 queue.py::add。
func (q *MemoryQueue) Add(ctx *ConversationContext) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.enabled {
		return
	}
	if ctx.Timestamp.IsZero() {
		ctx.Timestamp = time.Now().UTC()
	}
	q.enqueueLocked(ctx)
	q.resetTimerLocked()
}

// AddNowait 入队并立即后台处理。对应 queue.py::add_nowait。
func (q *MemoryQueue) AddNowait(ctx *ConversationContext) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.enabled {
		return
	}
	if ctx.Timestamp.IsZero() {
		ctx.Timestamp = time.Now().UTC()
	}
	q.enqueueLocked(ctx)
	q.scheduleTimerLocked(0)
}

// enqueueLocked 对应 _enqueue_locked:按 key 合并,信号 OR 合并,新上下文覆盖旧上下文。
func (q *MemoryQueue) enqueueLocked(ctx *ConversationContext) {
	key := memoryKey(ctx.ThreadID, ctx.UserID, ctx.AgentName)
	if existing, ok := q.queue[key]; ok {
		// OR 合并信号:即使后一次没检测到,窗口内「曾经」出现过就不丢。
		ctx.Correction = ctx.Correction || existing.Correction
		ctx.Reinforcement = ctx.Reinforcement || existing.Reinforcement
	}
	q.queue[key] = ctx
}

// resetTimerLocked 对应 _reset_timer:按 debounce 窗口重排定时器。
func (q *MemoryQueue) resetTimerLocked() {
	q.scheduleTimerLocked(q.debounce)
}

// scheduleTimerLocked 对应 _schedule_timer:取消旧 timer,排新 timer。
func (q *MemoryQueue) scheduleTimerLocked(delay time.Duration) {
	if q.timer != nil {
		q.timer.Stop()
	}
	q.timer = time.AfterFunc(delay, q.processQueue)
}

// processQueue 对应 _process_queue:批量处理队列(由定时器触发)。
func (q *MemoryQueue) processQueue() {
	q.mu.Lock()
	if q.processing {
		// 已有 worker 在处理:重排一个 0 延迟定时器,保证「立即 flush」语义。
		q.scheduleTimerLocked(0)
		q.mu.Unlock()
		return
	}
	if len(q.queue) == 0 {
		q.mu.Unlock()
		return
	}
	q.processing = true
	batch := make([]*ConversationContext, 0, len(q.queue))
	for _, c := range q.queue {
		batch = append(batch, c)
	}
	q.queue = map[string]*ConversationContext{}
	q.timer = nil
	q.mu.Unlock()

	// 批量处理。per-context 隔离 + 0.5s 限速在 MemoryUpdater.ProcessBatch 里实现
	// (对应 deer-flow 里 _process_queue 的 per-context try/except + time.sleep)。
	if q.process != nil {
		q.process(batch)
	}

	q.mu.Lock()
	q.processing = false
	q.mu.Unlock()
}

// Flush 强制立即同步处理队列。对应 flush(测试 / 优雅关闭)。
func (q *MemoryQueue) Flush() {
	q.mu.Lock()
	if q.timer != nil {
		q.timer.Stop()
		q.timer = nil
	}
	q.mu.Unlock()
	q.processQueue()
}

// FlushNowait 立即在后台启动处理。对应 flush_nowait。
func (q *MemoryQueue) FlushNowait() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.scheduleTimerLocked(0)
}

// Clear 清空队列且不处理。对应 clear(测试用)。
func (q *MemoryQueue) Clear() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.timer != nil {
		q.timer.Stop()
		q.timer = nil
	}
	q.queue = map[string]*ConversationContext{}
	q.processing = false
}

// PendingCount 返回待处理数量。对应 pending_count。
func (q *MemoryQueue) PendingCount() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.queue)
}

// IsProcessing 返回队列是否正在处理。对应 is_processing。
func (q *MemoryQueue) IsProcessing() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.processing
}

// 全局单例(对应 get_memory_queue / reset_memory_queue)。
var (
	memoryQueueMu sync.Mutex
	memoryQueue   *MemoryQueue
)

// GetMemoryQueue 返回全局记忆队列单例。
func GetMemoryQueue() *MemoryQueue {
	memoryQueueMu.Lock()
	defer memoryQueueMu.Unlock()
	if memoryQueue == nil {
		memoryQueue = NewMemoryQueueWithConfig(getMemoryConfig(), nil)
	}
	return memoryQueue
}

// ResetMemoryQueue 重置全局记忆队列(测试用)。
func ResetMemoryQueue() {
	memoryQueueMu.Lock()
	defer memoryQueueMu.Unlock()
	if memoryQueue != nil {
		memoryQueue.Clear()
	}
	memoryQueue = nil
}

// 全局记忆配置(对应 get_memory_config / set_memory_config)。
var (
	memoryConfigMu sync.Mutex
	memoryConfig   = DefaultMemoryConfig()
)

func getMemoryConfig() MemoryConfig {
	memoryConfigMu.Lock()
	defer memoryConfigMu.Unlock()
	return memoryConfig
}

// GetMemoryConfig 返回全局记忆配置。
func GetMemoryConfig() MemoryConfig { return getMemoryConfig() }

// SetMemoryConfig 设置全局记忆配置。
func SetMemoryConfig(cfg MemoryConfig) {
	memoryConfigMu.Lock()
	defer memoryConfigMu.Unlock()
	memoryConfig = cfg
}

// ── 消息处理(过滤 + 信号检测)──────────────────────────────────────────────
//
// message_processing.py 的过滤/检测逻辑与 summarization_hook.py 共用:compaction.go
// 里的 memory_flush_hook 需要 filter_messages_for_memory / detect_correction /
// detect_reinforcement,因此那份实现(含 uploadBlockRe / correctionPatterns /
// reinforcementPatterns 模式定义)落在 compaction.go。这里只暴露公开 API 并补上
// extract_message_text 的等价物,避免同包重复定义。

// ExtractMessageText 提取消息文本(对应 extract_message_text)。
// deer-flow 里 content 可能是 list(多模态),会拼接各 text 块;Go 的
// capability.Message.Content 已是 string,list 展开在上游完成。
func ExtractMessageText(content string) string {
	return content
}

// FilterMessagesForMemory 只保留 user 输入与最终 assistant 回复(对应 filter_messages_for_memory)。
func FilterMessagesForMemory(messages []capability.Message) []capability.Message {
	return filterMessagesForMemory(messages)
}

// DetectCorrection 检测显式修正信号(对应 detect_correction)。
func DetectCorrection(messages []capability.Message) bool {
	return detectCorrection(messages)
}

// DetectReinforcement 检测显式正向强化信号(对应 detect_reinforcement)。
func DetectReinforcement(messages []capability.Message) bool {
	return detectReinforcement(messages)
}

// ── 提示词与格式化 ──────────────────────────────────────────────────────────

// memoryUpdatePrompt 对应 MEMORY_UPDATE_PROMPT。占位符 {current_memory} / {conversation} /
// {correction_hint} 由 prepareUpdatePrompt 替换;JSON 示例用单花括号(Go 无 .format 转义)。
const memoryUpdatePrompt = `You are a memory management system. Your task is to analyze a conversation and update the user's memory profile.

Current Memory State:
<current_memory>
{current_memory}
</current_memory>

New Conversation to Process:
<conversation>
{conversation}
</conversation>

Instructions:
1. Analyze the conversation for important information about the user
2. Extract relevant facts, preferences, and context with specific details (numbers, names, technologies)
3. Update the memory sections as needed following the detailed length guidelines below

Before extracting facts, perform a structured reflection on the conversation:
1. Error/Retry Detection: Did the agent encounter errors, require retries, or produce incorrect results?
   If yes, record the root cause and correct approach as a high-confidence fact with category "correction".
2. User Correction Detection: Did the user correct the agent's direction, understanding, or output?
   If yes, record the correct interpretation or approach as a high-confidence fact with category "correction".
   Include what went wrong in "sourceError" only when category is "correction" and the mistake is explicit in the conversation.
3. Project Constraint Discovery: Were any project-specific constraints discovered during the conversation?
   If yes, record them as facts with the most appropriate category and confidence.

{correction_hint}

Output Format (JSON):
{
  "user": {
    "workContext": { "summary": "...", "shouldUpdate": true/false },
    "personalContext": { "summary": "...", "shouldUpdate": true/false },
    "topOfMind": { "summary": "...", "shouldUpdate": true/false }
  },
  "history": {
    "recentMonths": { "summary": "...", "shouldUpdate": true/false },
    "earlierContext": { "summary": "...", "shouldUpdate": true/false },
    "longTermBackground": { "summary": "...", "shouldUpdate": true/false }
  },
  "newFacts": [
    { "content": "...", "category": "preference|knowledge|context|behavior|goal|correction", "confidence": 0.0-1.0 }
  ],
  "factsToRemove": ["fact_id_1", "fact_id_2"]
}

Important Rules:
- Only set shouldUpdate=true if there's meaningful new information
- Only add facts that are clearly stated (0.9+) or strongly implied (0.7+)
- Use category "correction" for explicit agent mistakes or user corrections; assign confidence >= 0.95 when the correction is explicit
- Include "sourceError" only for explicit correction facts when the prior mistake or wrong approach is clearly stated; omit it otherwise
- Remove facts that are contradicted by new information
- IMPORTANT: Do NOT record file upload events in memory.

Return ONLY valid JSON, no explanation or markdown.`

// truncateWithEllipsis 按 Unicode 码点截断并追加 "..."(对应 str(content)[:1000] + "…" 的
// format_conversation_for_update 行为)。与 dangling.go 的 truncateRunes(不追加省略号)区分。
func truncateWithEllipsis(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "..."
}

// FormatConversationForUpdate 把消息格式化成「User: ... / Assistant: ...」文本。
// 对应 prompt.py::format_conversation_for_update。
func FormatConversationForUpdate(messages []capability.Message) string {
	lines := make([]string, 0, len(messages))
	for _, msg := range messages {
		content := msg.Content
		if msg.Role == "user" {
			content = strings.TrimSpace(uploadBlockRe.ReplaceAllString(content, ""))
			if content == "" {
				continue // 纯上传消息,整轮跳过
			}
		}
		if len([]rune(content)) > 1000 {
			content = truncateWithEllipsis(content, 1000)
		}
		switch msg.Role {
		case "user":
			lines = append(lines, "User: "+content)
		case "assistant":
			lines = append(lines, "Assistant: "+content)
		}
	}
	return strings.Join(lines, "\n\n")
}

// jsonIndent 紧凑 + 缩进的 JSON(对应 json.dumps(..., indent=2))。
func jsonIndent(v any) string {
	b, _ := json.MarshalIndent(v, "", "  ")
	return string(b)
}

// buildCorrectionHint 构建修正/强化提示(对应 _build_correction_hint)。
func buildCorrectionHint(correction, reinforcement bool) string {
	correctionHint := ""
	if correction {
		correctionHint = "IMPORTANT: Explicit correction signals were detected in this conversation. " +
			"Pay special attention to what the agent got wrong, what the user corrected, " +
			"and record the correct approach as a fact with category " +
			"\"correction\" and confidence >= 0.95 when appropriate."
	}
	if reinforcement {
		reinforcementHint := "IMPORTANT: Positive reinforcement signals were detected in this conversation. " +
			"The user explicitly confirmed the agent's approach was correct or helpful. " +
			"Record the confirmed approach, style, or preference as a fact with category " +
			"\"preference\" or \"behavior\" and confidence >= 0.9 when appropriate."
		if correctionHint != "" {
			correctionHint = correctionHint + "\n" + reinforcementHint
		} else {
			correctionHint = reinforcementHint
		}
	}
	return correctionHint
}

// ── 更新器(LLM 抽取事实)────────────────────────────────────────────────────

// MemoryModel 是记忆更新用的模型抽象(对应 model.invoke)。
type MemoryModel interface {
	Invoke(ctx context.Context, prompt string) (string, error)
}

// MemoryUpdater 使用 LLM 更新记忆。对应 updater.py::MemoryUpdater。
// 配置每次从全局 get_memory_config() 读取(对应 deer-flow 里每个方法都调
// get_memory_config() 取当前值),因此 SetMemoryConfig 之后立刻生效。
type MemoryUpdater struct {
	ModelName string
	Model     MemoryModel
	Storage   MemoryStorage
}

// NewMemoryUpdater 构造更新器。
func NewMemoryUpdater(model MemoryModel) *MemoryUpdater {
	return &MemoryUpdater{
		Model:   model,
		Storage: GetMemoryStorage(),
	}
}

func (u *MemoryUpdater) storage() MemoryStorage {
	if u.Storage != nil {
		return u.Storage
	}
	return GetMemoryStorage()
}

// UpdateMemory 同步更新记忆(对应 update_memory / _do_update_memory_sync)。
// 身份字段显式传入,不依赖跨 goroutine 的上下文传播。
func (u *MemoryUpdater) UpdateMemory(ctx context.Context, messages []capability.Message, threadID, agentName, userID string, correctionDetected, reinforcementDetected bool) bool {
	return u.doUpdateMemorySync(ctx, messages, threadID, agentName, userID, correctionDetected, reinforcementDetected)
}

// UpdateMemoryContext 是 UpdateMemory 的 *ConversationContext 便捷入口。
func (u *MemoryUpdater) UpdateMemoryContext(ctx context.Context, conv *ConversationContext) bool {
	if conv == nil {
		return false
	}
	return u.doUpdateMemorySync(ctx, conv.Messages, conv.ThreadID, conv.AgentName, conv.UserID, conv.Correction, conv.Reinforcement)
}

// ProcessBatch 批量处理(供 MemoryQueue.process 回调)。
// 对应 _process_queue 里的 per-context try/except + 0.5s 限速。
func (u *MemoryUpdater) ProcessBatch(batch []*ConversationContext) {
	for _, conv := range batch {
		// 每个 context 独立更新;UpdateMemory 内部已捕获错误并返回 bool,
		// 单个失败不影响批次(对应 per-context try/except)。
		u.UpdateMemoryContext(context.Background(), conv)
		if len(batch) > 1 {
			time.Sleep(500 * time.Millisecond) // 限速,避免打爆供应商
		}
	}
}

func (u *MemoryUpdater) doUpdateMemorySync(ctx context.Context, messages []capability.Message, threadID, agentName, userID string, correction, reinforcement bool) bool {
	cfg := getMemoryConfig()
	currentMemory, prompt, ok := u.prepareUpdatePrompt(messages, agentName, correction, reinforcement, userID, cfg)
	if !ok {
		return false
	}
	if u.Model == nil {
		return false
	}
	response, err := u.Model.Invoke(ctx, prompt)
	if err != nil {
		return false
	}
	return u.finalizeUpdate(currentMemory, response, threadID, agentName, userID, cfg)
}

// prepareUpdatePrompt 加载记忆并构建更新提示(对应 _prepare_update_prompt)。
func (u *MemoryUpdater) prepareUpdatePrompt(messages []capability.Message, agentName string, correction, reinforcement bool, userID string, cfg MemoryConfig) (MemoryData, string, bool) {
	if !cfg.Enabled || len(messages) == 0 {
		return nil, "", false
	}
	current, err := u.storage().Load(agentName, userID)
	if err != nil {
		return nil, "", false
	}
	conversation := FormatConversationForUpdate(messages)
	if strings.TrimSpace(conversation) == "" {
		return nil, "", false
	}
	hint := buildCorrectionHint(correction, reinforcement)
	prompt := strings.ReplaceAll(memoryUpdatePrompt, "{current_memory}", jsonIndent(current))
	prompt = strings.ReplaceAll(prompt, "{conversation}", conversation)
	prompt = strings.ReplaceAll(prompt, "{correction_hint}", hint)
	return current, prompt, true
}

// finalizeUpdate 解析模型响应、应用更新并持久化(对应 _finalize_update)。
func (u *MemoryUpdater) finalizeUpdate(current MemoryData, responseContent, threadID, agentName, userID string, cfg MemoryConfig) bool {
	updateData, err := parseMemoryUpdateResponse(responseContent)
	if err != nil {
		return false // JSONDecodeError -> False
	}
	updated := u.applyUpdates(deepCopyMemory(current), updateData, threadID, cfg)
	updated = stripUploadMentions(updated)
	ok, _ := u.storage().Save(updated, agentName, userID)
	return ok
}

// ── 归一化与解析 ────────────────────────────────────────────────────────────

// requiredMemoryUpdateKeys 对应 _REQUIRED_MEMORY_UPDATE_TOP_LEVEL_KEYS。
var requiredMemoryUpdateKeys = map[string]bool{
	"user": true, "history": true, "newFacts": true, "factsToRemove": true,
}

// decodeJSONValue 解析字符串开头的第一个 JSON 值(对应 json.JSONDecoder().raw_decode)。
// Go 的 json.Decoder.Decode 读第一个值并忽略其后内容,语义等价。
func decodeJSONValue(s string) (any, error) {
	dec := json.NewDecoder(strings.NewReader(s))
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	return v, nil
}

// hasRequiredKeys 判断顶层键是否齐备。
func hasRequiredKeys(m map[string]any) bool {
	for k := range requiredMemoryUpdateKeys {
		if _, ok := m[k]; !ok {
			return false
		}
	}
	return true
}

// normalizeMemoryUpdateFact 归一化单条 fact(对应 _normalize_memory_update_fact)。
// 非法条目返回 nil。
func normalizeMemoryUpdateFact(fact any) map[string]any {
	m, ok := fact.(map[string]any)
	if !ok {
		return nil
	}
	rawContent, ok := m["content"].(string)
	if !ok {
		return nil
	}
	content := strings.TrimSpace(rawContent)
	if content == "" {
		return nil
	}

	rawCategory, _ := m["category"].(string)
	category := strings.TrimSpace(rawCategory)
	if category == "" {
		category = "context"
	}

	confidence, ok := coerceRawConfidence(m["confidence"])
	if !ok {
		return nil
	}

	normalized := map[string]any{
		"content":    content,
		"category":   category,
		"confidence": confidence,
	}
	if se, ok := m["sourceError"].(string); ok {
		if s := strings.TrimSpace(se); s != "" {
			normalized["sourceError"] = s
		}
	}
	return normalized
}

// coerceRawConfidence 把置信度原始值转成 float64;bool 视为非法(对应 _normalize_memory_update_fact)。
func coerceRawConfidence(v any) (float64, bool) {
	switch c := v.(type) {
	case nil:
		return 0.5, true
	case bool:
		return 0, false
	case float64:
		if math.IsNaN(c) || math.IsInf(c, 0) {
			return 0, false
		}
		return c, true
	case int:
		return float64(c), true
	case string:
		s := strings.TrimSpace(c)
		if s == "" {
			return 0, false
		}
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return 0, false
		}
		return f, true
	default:
		return 0, false
	}
}

// normalizeMemoryUpdateData 把解析出的更新数据强制成 _apply_updates 期望的形状。
// 对应 _normalize_memory_update_data;factsToRemove 非空但 newFacts 畸形时报错。
func normalizeMemoryUpdateData(update map[string]any) (map[string]any, error) {
	user, _ := update["user"].(map[string]any)
	if user == nil {
		user = map[string]any{}
	}
	history, _ := update["history"].(map[string]any)
	if history == nil {
		history = map[string]any{}
	}

	var factsToRemove []string
	if fr, ok := update["factsToRemove"].([]any); ok {
		for _, id := range fr {
			if s, ok := id.(string); ok {
				factsToRemove = append(factsToRemove, s)
			}
		}
	}

	var newFacts []map[string]any
	droppedNewFact := false
	if nf, ok := update["newFacts"].([]any); ok {
		for _, f := range nf {
			if n := normalizeMemoryUpdateFact(f); n != nil {
				newFacts = append(newFacts, n)
			} else {
				droppedNewFact = true
			}
		}
	} else {
		droppedNewFact = true
	}

	if len(factsToRemove) > 0 && droppedNewFact {
		return nil, fmt.Errorf("unsafe partial memory update: factsToRemove with malformed newFacts")
	}

	return map[string]any{
		"user":          user,
		"history":       history,
		"newFacts":      newFacts,
		"factsToRemove": factsToRemove,
	}, nil
}

// parseMemoryUpdateResponse 从 LLM 响应里解析第一个有效的记忆更新 JSON。
// 对应 _parse_memory_update_response:容忍 thinking trace / prose / markdown 包裹,
// 但不修复截断或畸形 JSON。
func parseMemoryUpdateResponse(content string) (map[string]any, error) {
	text := strings.TrimSpace(content)
	for i := 0; i < len(text); i++ {
		if text[i] != '{' {
			continue
		}
		parsed, err := decodeJSONValue(text[i:])
		if err != nil {
			continue
		}
		m, ok := parsed.(map[string]any)
		if !ok || !hasRequiredKeys(m) {
			continue
		}
		return normalizeMemoryUpdateData(m)
	}
	return nil, fmt.Errorf("no valid memory update JSON object found")
}

// deepCopyValue 深拷贝 map/slice(对应 copy.deepcopy)。
func deepCopyValue(v any) any {
	switch t := v.(type) {
	case MemoryData:
		return deepCopyMemory(t)
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			out[k] = deepCopyValue(val)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, val := range t {
			out[i] = deepCopyValue(val)
		}
		return out
	default:
		return v
	}
}

// deepCopyMemory 深拷贝记忆(保存失败时不污染仍被缓存的原始对象引用)。
func deepCopyMemory(m MemoryData) MemoryData {
	if m == nil {
		return nil
	}
	out := make(MemoryData, len(m))
	for k, v := range m {
		out[k] = deepCopyValue(v)
	}
	return out
}

// ── 应用更新 ────────────────────────────────────────────────────────────────

// uploadSentenceRe 对应 _UPLOAD_SENTENCE_RE:匹配「文件上传事件」的句子。
// 刻意收窄,避免误删「User works with CSV files」这类正常事实。
var uploadSentenceRe = regexp.MustCompile(`(?i)[^.!?]*\b(?:upload(?:ed|ing)?(?:\s+\w+){0,3}\s+(?:file|files?|document|documents?|attachment|attachments?)|file\s+upload|/mnt/user-data/uploads/|<uploaded_files>)[^.!?]*[.!?]?\s*`)

// collapseSpacesRe 对应 re.sub(r"  +", " ", ...)。
var collapseSpacesRe = regexp.MustCompile(`  +`)

// stripUploadMentions 从所有记忆摘要与 facts 里移除文件上传句子(对应同名函数)。
func stripUploadMentions(memory MemoryData) MemoryData {
	for _, section := range []string{"user", "history"} {
		sectionData, _ := memory[section].(map[string]any)
		for _, v := range sectionData {
			vm, ok := v.(map[string]any)
			if !ok {
				continue
			}
			if summary, ok := vm["summary"].(string); ok {
				cleaned := uploadSentenceRe.ReplaceAllString(summary, "")
				cleaned = strings.TrimSpace(cleaned)
				cleaned = collapseSpacesRe.ReplaceAllString(cleaned, " ")
				vm["summary"] = cleaned
			}
		}
	}

	facts := currentFacts(memory)
	if len(facts) > 0 {
		kept := make([]any, 0, len(facts))
		for _, f := range facts {
			fm, ok := f.(map[string]any)
			if ok {
				if content, ok := fm["content"].(string); ok && uploadSentenceRe.MatchString(content) {
					continue
				}
			}
			kept = append(kept, f)
		}
		memory["facts"] = kept
	}
	return memory
}

// factContentKey 对应 _fact_content_key:内容去重键(casefold 近似)。
func factContentKey(content any) string {
	s, ok := content.(string)
	if !ok {
		return ""
	}
	trimmed := strings.TrimSpace(s)
	if trimmed == "" {
		return ""
	}
	return strings.ToLower(trimmed)
}

// factConfidence 取 fact 的置信度(缺省 0)。
func factConfidence(f any) float64 {
	fm, ok := f.(map[string]any)
	if !ok {
		return 0
	}
	c, _ := fm["confidence"].(float64)
	return c
}

// factID 取 fact 的 id。
func factID(f map[string]any) string {
	id, _ := f["id"].(string)
	return id
}

// applyUpdates 应用 LLM 生成的更新(对应 _apply_updates)。
func (u *MemoryUpdater) applyUpdates(current MemoryData, update map[string]any, threadID string, cfg MemoryConfig) MemoryData {
	now := utcNowISOZ()

	// user 段。
	if userContainer, ok := current["user"].(map[string]any); ok {
		userUpdates, _ := update["user"].(map[string]any)
		for _, section := range []string{"workContext", "personalContext", "topOfMind"} {
			setSummaryIfUpdated(userContainer, userUpdates, section, now)
		}
	}
	// history 段。
	if historyContainer, ok := current["history"].(map[string]any); ok {
		historyUpdates, _ := update["history"].(map[string]any)
		for _, section := range []string{"recentMonths", "earlierContext", "longTermBackground"} {
			setSummaryIfUpdated(historyContainer, historyUpdates, section, now)
		}
	}

	// 删除 facts。
	if fr, ok := update["factsToRemove"].([]string); ok && len(fr) > 0 {
		removeSet := stringSet(fr)
		facts := currentFacts(current)
		kept := make([]any, 0, len(facts))
		for _, f := range facts {
			fm, ok := f.(map[string]any)
			if ok && removeSet[factID(fm)] {
				continue
			}
			kept = append(kept, f)
		}
		current["facts"] = kept
	}

	// 新增 facts。
	existingKeys := map[string]bool{}
	for _, f := range currentFacts(current) {
		if fm, ok := f.(map[string]any); ok {
			if k := factContentKey(fm["content"]); k != "" {
				existingKeys[k] = true
			}
		}
	}
	facts := currentFacts(current)
	if newFacts, ok := update["newFacts"].([]map[string]any); ok {
		for _, fact := range newFacts {
			confidence, hasConf := fact["confidence"].(float64)
			if !hasConf {
				confidence = 0.5
			}
			if confidence < cfg.FactConfidenceThreshold {
				continue
			}
			rawContent, ok := fact["content"].(string)
			if !ok {
				continue
			}
			content := strings.TrimSpace(rawContent)
			key := factContentKey(content)
			if key != "" && existingKeys[key] {
				continue
			}

			category, _ := fact["category"].(string)
			if category == "" {
				category = "context"
			}
			source := threadID
			if source == "" {
				source = "unknown"
			}
			entry := map[string]any{
				"id":         "fact_" + randomHex(8),
				"content":    content,
				"category":   category,
				"confidence": confidence,
				"createdAt":  now,
				"source":     source,
			}
			if se, ok := fact["sourceError"].(string); ok {
				if s := strings.TrimSpace(se); s != "" {
					entry["sourceError"] = s
				}
			}
			facts = append(facts, entry)
			if key != "" {
				existingKeys[key] = true
			}
		}
	}

	// 上限裁剪:按置信度降序保留 top MaxFacts。
	if len(facts) > cfg.MaxFacts {
		sort.SliceStable(facts, func(i, j int) bool {
			return factConfidence(facts[i]) > factConfidence(facts[j])
		})
		facts = facts[:cfg.MaxFacts]
	}
	current["facts"] = facts

	return current
}

// setSummaryIfUpdated 若 shouldUpdate 且 summary 非空,则覆盖该段摘要。
func setSummaryIfUpdated(container, updates map[string]any, section, now string) {
	if updates == nil {
		return
	}
	sec, ok := updates[section].(map[string]any)
	if !ok {
		return
	}
	shouldUpdate, _ := sec["shouldUpdate"].(bool)
	summary, _ := sec["summary"].(string)
	if shouldUpdate && summary != "" {
		container[section] = map[string]any{"summary": summary, "updatedAt": now}
	}
}

// stringSet 把切片转集合。
func stringSet(names []string) map[string]bool {
	out := make(map[string]bool, len(names))
	for _, n := range names {
		out[n] = true
	}
	return out
}

// randomHex 生成 n 个随机 hex 字符(对应 uuid.uuid4().hex[:n])。
func randomHex(n int) string {
	b := make([]byte, (n+1)/2)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%08x", time.Now().UnixNano())[:n]
	}
	return hex.EncodeToString(b)[:n]
}

// ── 记忆事实 CRUD(对应 updater.py 模块级函数)────────────────────────────────

// validateConfidence 校验置信度(对应 _validate_confidence)。
func validateConfidence(confidence float64) (float64, error) {
	if math.IsNaN(confidence) || math.IsInf(confidence, 0) || confidence < 0 || confidence > 1 {
		return 0, fmt.Errorf("confidence out of range [0,1]: %v", confidence)
	}
	return confidence, nil
}

// GetMemoryData 获取当前记忆(对应 get_memory_data)。
func GetMemoryData(agentName, userID string) MemoryData {
	data, _ := GetMemoryStorage().Load(agentName, userID)
	return data
}

// ReloadMemoryData 强制重载记忆(对应 reload_memory_data)。
func ReloadMemoryData(agentName, userID string) MemoryData {
	data, _ := GetMemoryStorage().Reload(agentName, userID)
	return data
}

// ImportMemoryData 持久化导入的记忆(对应 import_memory_data)。
func ImportMemoryData(data MemoryData, agentName, userID string) (MemoryData, error) {
	ok, err := GetMemoryStorage().Save(data, agentName, userID)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("failed to save imported memory data")
	}
	out, _ := GetMemoryStorage().Load(agentName, userID)
	return out, nil
}

// ClearMemoryData 清空并持久化空结构(对应 clear_memory_data)。
func ClearMemoryData(agentName, userID string) (MemoryData, error) {
	cleared := CreateEmptyMemory()
	ok, err := GetMemoryStorage().Save(cleared, agentName, userID)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("failed to save cleared memory data")
	}
	return cleared, nil
}

// CreateMemoryFact 新建一条 fact 并持久化(对应 create_memory_fact)。
func CreateMemoryFact(content, category string, confidence float64, agentName, userID string) (MemoryData, error) {
	normalized := strings.TrimSpace(content)
	if normalized == "" {
		return nil, fmt.Errorf("content must be non-empty")
	}
	if strings.TrimSpace(category) == "" {
		category = "context"
	}
	validated, err := validateConfidence(confidence)
	if err != nil {
		return nil, err
	}
	now := utcNowISOZ()
	memoryData := GetMemoryData(agentName, userID)
	updated := deepCopyMemory(memoryData)
	facts := append(currentFacts(updated), map[string]any{
		"id":         "fact_" + randomHex(8),
		"content":    normalized,
		"category":   category,
		"confidence": validated,
		"createdAt":  now,
		"source":     "manual",
	})
	updated["facts"] = facts
	ok, err := GetMemoryStorage().Save(updated, agentName, userID)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("failed to save memory data after creating fact")
	}
	return updated, nil
}

// DeleteMemoryFact 按 id 删除 fact 并持久化(对应 delete_memory_fact)。
func DeleteMemoryFact(factIDToDelete, agentName, userID string) (MemoryData, error) {
	memoryData := GetMemoryData(agentName, userID)
	facts := currentFacts(memoryData)
	updatedFacts := make([]any, 0, len(facts))
	for _, f := range facts {
		fm, ok := f.(map[string]any)
		if ok && factID(fm) == factIDToDelete {
			continue
		}
		updatedFacts = append(updatedFacts, f)
	}
	if len(updatedFacts) == len(facts) {
		return nil, fmt.Errorf("fact not found: %s", factIDToDelete)
	}
	updated := deepCopyMemory(memoryData)
	updated["facts"] = updatedFacts
	ok, err := GetMemoryStorage().Save(updated, agentName, userID)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("failed to save memory data after deleting fact '%s'", factIDToDelete)
	}
	return updated, nil
}

// UpdateMemoryFact 更新一条 fact 并持久化(对应 update_memory_fact)。
// 传 nil 表示不更新该字段。
func UpdateMemoryFact(factIDToUpdate string, content, category *string, confidence *float64, agentName, userID string) (MemoryData, error) {
	memoryData := GetMemoryData(agentName, userID)
	facts := currentFacts(memoryData)
	updatedFacts := make([]any, 0, len(facts))
	found := false
	for _, f := range facts {
		fm, ok := f.(map[string]any)
		if !ok || factID(fm) != factIDToUpdate {
			updatedFacts = append(updatedFacts, f)
			continue
		}
		found = true
		nf := deepCopyValue(fm).(map[string]any)
		if content != nil {
			normalized := strings.TrimSpace(*content)
			if normalized == "" {
				return nil, fmt.Errorf("content must be non-empty")
			}
			nf["content"] = normalized
		}
		if category != nil {
			cat := strings.TrimSpace(*category)
			if cat == "" {
				cat = "context"
			}
			nf["category"] = cat
		}
		if confidence != nil {
			validated, err := validateConfidence(*confidence)
			if err != nil {
				return nil, err
			}
			nf["confidence"] = validated
		}
		updatedFacts = append(updatedFacts, nf)
	}
	if !found {
		return nil, fmt.Errorf("fact not found: %s", factIDToUpdate)
	}
	updated := deepCopyMemory(memoryData)
	updated["facts"] = updatedFacts
	ok, err := GetMemoryStorage().Save(updated, agentName, userID)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("failed to save memory data after updating fact '%s'", factIDToUpdate)
	}
	return updated, nil
}
