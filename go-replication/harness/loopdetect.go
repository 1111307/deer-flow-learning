// 循环检测 + 安全兜底 —— 对应 deer-flow agents/middlewares/loop_detection_middleware.py
// (整文件 613 行)。
//
// 源码行号映射:
//   - 默认常量            : loop_detection_middleware.py:63-70
//   - _normalize_tool_call_args : :73-96
//   - _stable_tool_key    : :99-139(read_file 按 200 行分桶、write_file/str_replace 全参哈希、salient 字段)
//   - _hash_tool_calls    : :142-160(顺序无关 md5[:12])
//   - 消息模板            : :163-171
//   - LoopDetectionMiddleware.__init__ : :205-233(含 per-thread 追踪 + pending 警告队列)
//   - _evict_if_needed    : :266-279(LRU 淘汰)
//   - _track_and_check    : :322-438(两层检测)
//   - _apply              : :477-502(硬停止清空 tool_calls / 警告排队)
//   - wrap_model_call     : :579-585(警告延迟注入)
//
// 核心设计(两层检测 + 梯度处理):
//   - Layer 1 哈希去重:完全相同的调用集出现 warn(3) 次排队警告、hard(5) 次硬停止。
//   - Layer 2 工具频率:参数不同但同类工具狂调(如 read_file 扫 40 个文件)兜底,
//     warn(30)/hard(50),可按工具名覆盖阈值。
//   - 滑动窗口(20)只统计最近 N 次调用;LRU(100 线程)防止跨线程状态无限增长。
//   - 警告「延迟注入」:after_model 阶段不能插消息(会破坏 assistant.tool_calls 与
//     tool 结果的配对,OpenAI/Anthropic 会 400),必须排队,到下一次模型调用前
//     (wrap_model_call)追加到消息列表末尾。
//
// Go 与 Python 的关键差异:
//   - Python 的 OrderedDict + move_to_end 做 LRU;Go 用 map + 顺序切片表达同样的
//     「最近使用移到队尾、淘汰队首」。
//   - Python 的 (thread_id, run_id) 元组键;Go 用 pendingKey 结构体。
//   - Python 的 threading.Lock;Go 用 sync.Mutex。
//   - LangGraph 的 Runtime.context 提供 thread_id/run_id;Go 由调用方显式传入。
//   - _append_text 的 list 分支(Anthropic content blocks)在 Go 的 string-only
//     Message 模型里不适用,合并为「空串当作 None」。
package harness

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"

	"deerflow-go/capability"
)

// 默认阈值 —— 对应 loop_detection_middleware.py:63-70。
const (
	defaultWarnThreshold     = 3   // 相同调用集出现 3 次注入警告
	defaultHardLimit         = 5   // 相同调用集出现 5 次强制停止
	defaultWindowSize        = 20  // 滑动窗口:只统计最近 20 次调用
	defaultMaxTrackedThreads = 100 // LRU 淘汰上限
	defaultToolFreqWarn      = 30  // 同类工具调用 30 次注入频率警告
	defaultToolFreqHardLimit = 50  // 同类工具调用 50 次强制停止
	maxPendingWarningsPerRun = 4   // 每个 run 排队警告上限
)

// 消息模板 —— 对应 loop_detection_middleware.py:163-171(逐字对齐,含 em-dash)。
const (
	loopWarningMsg = "[LOOP DETECTED] You are repeating the same tool calls. Stop calling tools and produce your final answer now. If you cannot complete the task, summarize what you accomplished so far."

	loopFreqWarningMsg = "[LOOP DETECTED] You have called %s %d times without producing a final answer. Stop calling tools and produce your final answer now. If you cannot complete the task, summarize what you accomplished so far."

	loopHardStopMsg = "[FORCED STOP] Repeated tool calls exceeded the safety limit. Producing final answer with results collected so far."

	loopFreqHardStopMsg = "[FORCED STOP] Tool %s called %d times — exceeded the per-tool safety limit. Producing final answer with results collected so far."
)

// FreqThresholds 是某个工具的 (warn, hard) 频率阈值对。
// 对应 LoopDetectionConfig.tool_freq_overrides 的 (warn, hard_limit) 元组。
type FreqThresholds struct {
	Warn int
	Hard int
}

// LoopDetectionConfig 是循环检测中间件的构造参数。
// 对应 LoopDetectionMiddleware.__init__ 的 7 个参数(Pydantic 校验在 deer-flow 上层)。
type LoopDetectionConfig struct {
	WarnThreshold     int                       // 相同调用集警告阈值(默认 3)
	HardLimit         int                       // 相同调用集硬停止阈值(默认 5)
	WindowSize        int                       // 滑动窗口大小(默认 20)
	MaxTrackedThreads int                       // LRU 追踪线程上限(默认 100)
	ToolFreqWarn      int                       // 工具频率警告阈值(默认 30)
	ToolFreqHardLimit int                       // 工具频率硬停止阈值(默认 50)
	ToolFreqOverrides map[string]FreqThresholds // 按工具名覆盖频率阈值(默认 nil)
}

// DefaultLoopDetectionConfig 返回默认配置(对应 __init__ 的默认参数)。
func DefaultLoopDetectionConfig() LoopDetectionConfig {
	return LoopDetectionConfig{
		WarnThreshold:     defaultWarnThreshold,
		HardLimit:         defaultHardLimit,
		WindowSize:        defaultWindowSize,
		MaxTrackedThreads: defaultMaxTrackedThreads,
		ToolFreqWarn:      defaultToolFreqWarn,
		ToolFreqHardLimit: defaultToolFreqHardLimit,
	}
}

// pendingKey 对应 Python 的 (thread_id, run_id) 元组,用于按 thread/run 隔离警告队列。
type pendingKey struct {
	threadID string
	runID    string
}

// LoopDetector 检测并打断重复工具调用循环(对应 LoopDetectionMiddleware)。
//
// 并发安全:所有可变状态都由 mu 保护。单个 LoopDetector 可被多个并发 run/thread
// 共享(对应 deer-flow 里中间件实例是跨 run 共享的单例)。
type LoopDetector struct {
	mu sync.Mutex

	WarnThreshold     int
	HardLimit         int
	WindowSize        int
	MaxTrackedThreads int
	ToolFreqWarn      int
	ToolFreqHardLimit int
	toolFreqOverrides map[string]FreqThresholds

	// 按 thread_id 分区的检测状态(LRU):
	history        map[string][]string            // thread_id -> 调用哈希滑动窗口
	order          []string                       // thread_id 的 LRU 顺序(队首最久未用)
	warned         map[string]map[string]struct{} // thread_id -> 已警告的调用哈希
	toolFreq       map[string]map[string]int      // thread_id -> 工具名 -> 调用次数
	toolFreqWarned map[string]map[string]struct{} // thread_id -> 已警告的工具名

	// 按 (thread, run) 分区的排队警告:
	pending        map[pendingKey][]string
	pendingOrder   []pendingKey // pending key 的 LRU 顺序(队首最久未用)
	maxPendingKeys int
}

// NewLoopDetector 构造默认阈值的检测器(对应 __init__ 默认参数)。
func NewLoopDetector() *LoopDetector {
	return NewLoopDetectorWithConfig(DefaultLoopDetectionConfig())
}

// NewLoopDetectorWithConfig 按配置构造检测器(对应 from_config 直接透传已校验值)。
// 零值字段回退到默认值(对应 Python 构造函数的默认参数)。
func NewLoopDetectorWithConfig(cfg LoopDetectionConfig) *LoopDetector {
	if cfg.WarnThreshold <= 0 {
		cfg.WarnThreshold = defaultWarnThreshold
	}
	if cfg.HardLimit <= 0 {
		cfg.HardLimit = defaultHardLimit
	}
	if cfg.WindowSize <= 0 {
		cfg.WindowSize = defaultWindowSize
	}
	if cfg.MaxTrackedThreads <= 0 {
		cfg.MaxTrackedThreads = defaultMaxTrackedThreads
	}
	if cfg.ToolFreqWarn <= 0 {
		cfg.ToolFreqWarn = defaultToolFreqWarn
	}
	if cfg.ToolFreqHardLimit <= 0 {
		cfg.ToolFreqHardLimit = defaultToolFreqHardLimit
	}
	overrides := cfg.ToolFreqOverrides
	if overrides == nil {
		overrides = map[string]FreqThresholds{}
	}
	maxThreads := cfg.MaxTrackedThreads
	if maxThreads < 1 {
		maxThreads = 1
	}
	return &LoopDetector{
		WarnThreshold:     cfg.WarnThreshold,
		HardLimit:         cfg.HardLimit,
		WindowSize:        cfg.WindowSize,
		MaxTrackedThreads: maxThreads,
		ToolFreqWarn:      cfg.ToolFreqWarn,
		ToolFreqHardLimit: cfg.ToolFreqHardLimit,
		toolFreqOverrides: overrides,
		history:           map[string][]string{},
		warned:            map[string]map[string]struct{}{},
		toolFreq:          map[string]map[string]int{},
		toolFreqWarned:    map[string]map[string]struct{}{},
		pending:           map[pendingKey][]string{},
		maxPendingKeys:    max(1, maxThreads*2),
	}
}

// ---------------------------------------------------------------------------
// 兼容接口(harness/loop.go 的单线程调用路径)
// ---------------------------------------------------------------------------

// AfterModel 在模型每次返回 tool_calls 后调用,做两层检测(对应 after_model)。
// 这是单线程便捷接口:等价于 TrackAndCheck("default", "default", calls)。
// 返回 hardStop(是否强制停止);text 为硬停止消息(非硬停止时为空)。
// 警告会内部排队,由 DrainPending 在下次模型调用前注入。
func (d *LoopDetector) AfterModel(calls []capability.ToolCall) (hardStop bool, text string) {
	return d.TrackAndCheck("", "", calls)
}

// DrainPending 取出并清空排队的警告(下次模型调用前注入,对应 wrap_model_call)。
// 单线程便捷接口:等价于 DrainPendingWarnings("default", "default")。
func (d *LoopDetector) DrainPending() []string {
	return d.DrainPendingWarnings("", "")
}

// ---------------------------------------------------------------------------
// 忠实复现的 per-thread/run 接口(对应 deer-flow 的 runtime.context)
// ---------------------------------------------------------------------------

// BeforeAgent 清理同一 thread 上「其他 run」遗留的 pending 警告(对应 before_agent)。
// 在每次 run 开始时调用,防止上一 run 中断后残留的警告污染本次 run。
func (d *LoopDetector) BeforeAgent(threadID, runID string) {
	threadID = normalizeID(threadID)
	runID = normalizeID(runID)
	d.mu.Lock()
	defer d.mu.Unlock()
	for k := range d.pending {
		if k.threadID == threadID && k.runID != runID {
			d.dropPendingKeyLocked(k)
		}
	}
}

// AfterAgent 清理当前 thread/run 的 pending 警告(对应 after_agent)。
// run 正常结束(或硬停止)后调用,丢弃未排空的瞬时警告。
func (d *LoopDetector) AfterAgent(threadID, runID string) {
	k := pendingKey{normalizeID(threadID), normalizeID(runID)}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.dropPendingKeyLocked(k)
}

// TrackAndCheck 追踪 tool_calls 并做两层检测(对应 _track_and_check + _apply)。
//
//   - 硬停止:返回 (true, 硬停止消息)。
//   - 警告:内部排队(按 thread/run),返回 (false, "")。
//
// 警告不在本方法内注入消息 —— 由调用方在下次模型调用前用 DrainPendingWarnings 取出,
// 追加到消息列表末尾(保持 tool_calls 配对)。
func (d *LoopDetector) TrackAndCheck(threadID, runID string, calls []capability.ToolCall) (hardStop bool, text string) {
	if len(calls) == 0 {
		return false, ""
	}
	threadID = normalizeID(threadID)
	runID = normalizeID(runID)
	callHash := hashToolCalls(calls)

	d.mu.Lock()
	defer d.mu.Unlock()

	// 触碰/创建该 thread 的历史条目(LRU 移到队尾);新建时触发 LRU 淘汰。
	if _, ok := d.history[threadID]; ok {
		d.touchThreadLocked(threadID)
	} else {
		d.history[threadID] = []string{}
		d.order = append(d.order, threadID)
		d.evictIfNeededLocked()
	}

	hist := d.history[threadID]
	hist = append(hist, callHash)
	if len(hist) > d.WindowSize {
		hist = hist[len(hist)-d.WindowSize:]
	}
	d.history[threadID] = hist

	// 修剪已警告哈希集合,只保留仍在窗口内的(窗口滑出后可再次警告)。
	if warned := d.warned[threadID]; warned != nil {
		kept := map[string]struct{}{}
		for _, h := range hist {
			if _, ok := warned[h]; ok {
				kept[h] = struct{}{}
			}
		}
		if len(kept) == 0 {
			delete(d.warned, threadID)
		} else {
			d.warned[threadID] = kept
		}
	}

	count := 0
	for _, h := range hist {
		if h == callHash {
			count++
		}
	}

	// --- Layer 1:哈希去重(相同调用集) ---
	if count >= d.HardLimit {
		return true, loopHardStopMsg
	}
	if count >= d.WarnThreshold {
		warned := d.warned[threadID]
		if warned == nil {
			warned = map[string]struct{}{}
			d.warned[threadID] = warned
		}
		if _, ok := warned[callHash]; !ok {
			warned[callHash] = struct{}{}
			d.queueWarningLocked(threadID, runID, loopWarningMsg)
			return false, ""
		}
	}

	// --- Layer 2:按工具类型频率(参数不同但同类工具狂调) ---
	freq := d.toolFreq[threadID]
	if freq == nil {
		freq = map[string]int{}
		d.toolFreq[threadID] = freq
	}
	for _, tc := range calls {
		name := tc.Name
		if name == "" {
			continue
		}
		freq[name]++
		tcCount := freq[name]

		effWarn, effHard := d.effectiveFreq(name)
		if tcCount >= effHard {
			return true, fmt.Sprintf(loopFreqHardStopMsg, name, tcCount)
		}
		if tcCount >= effWarn {
			warned := d.toolFreqWarned[threadID]
			if warned == nil {
				warned = map[string]struct{}{}
				d.toolFreqWarned[threadID] = warned
			}
			if _, ok := warned[name]; !ok {
				warned[name] = struct{}{}
				d.queueWarningLocked(threadID, runID, fmt.Sprintf(loopFreqWarningMsg, name, tcCount))
				return false, ""
			}
		}
	}
	return false, ""
}

// DrainPendingWarnings 弹出当前 thread/run 的所有排队警告(对应 _drain_pending_warnings)。
func (d *LoopDetector) DrainPendingWarnings(threadID, runID string) []string {
	k := pendingKey{normalizeID(threadID), normalizeID(runID)}
	d.mu.Lock()
	defer d.mu.Unlock()
	warnings := d.pending[k]
	d.dropPendingKeyLocked(k)
	return warnings
}

// Reset 清空追踪状态(对应 reset)。
// threadID 为空表示清空全部;非空只清空该 thread 的所有状态(含 pending 警告)。
func (d *LoopDetector) Reset(threadID string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if threadID == "" {
		d.history = map[string][]string{}
		d.order = nil
		d.warned = map[string]map[string]struct{}{}
		d.toolFreq = map[string]map[string]int{}
		d.toolFreqWarned = map[string]map[string]struct{}{}
		d.pending = map[pendingKey][]string{}
		d.pendingOrder = nil
		return
	}
	delete(d.history, threadID)
	d.order = removeThreadFromOrder(d.order, threadID)
	delete(d.warned, threadID)
	delete(d.toolFreq, threadID)
	delete(d.toolFreqWarned, threadID)
	for k := range d.pending {
		if k.threadID == threadID {
			d.dropPendingKeyLocked(k)
		}
	}
}

// ---------------------------------------------------------------------------
// 消息构造辅助(对应 _append_text / _build_hard_stop_update / _format_warning_message)
// ---------------------------------------------------------------------------

// BuildHardStopMessage 构造硬停止后的 assistant 消息(对应 _apply 的 hard_stop 分支)。
//
//   - 清空 ToolCalls(逼模型输出纯文本收尾);
//   - 用 _append_text 把硬停止文本拼到原内容之后(保留已有的文本内容)。
//
// Python 的 _build_hard_stop_update 还会清理 additional_kwargs["tool_calls"] /
// ["function_call"]、把 response_metadata["finish_reason"] 从 "tool_calls" 改成 "stop",
// 这些是 LangChain 消息的内部字段,Go 的 capability.Message 没有,等价表达为
// 「清空 ToolCalls + 拼接内容」。
func BuildHardStopMessage(last capability.Message, text string) capability.Message {
	return capability.Message{
		Role:      last.Role,
		Content:   appendLoopText(last.Content, text),
		ToolCalls: nil,
	}
}

// appendLoopText 复现 _append_text 的 string/None 分支。
// Python 里 content 可能是 None / str / list(content blocks);Go 的 Message.Content
// 恒为 string,list 分支不适用,故「空串当作 None」。
func appendLoopText(content, text string) string {
	if content == "" {
		return text
	}
	return content + "\n\n" + text
}

// FormatWarnings 把排队警告合并成一条提示(对应 _format_warning_message):
// 去重 + "\n\n" 连接。
func FormatWarnings(warnings []string) string {
	return strings.Join(dedupeStrings(warnings), "\n\n")
}

// ---------------------------------------------------------------------------
// 内部实现(对应 _normalize_tool_call_args / _stable_tool_key / _hash_tool_calls)
// ---------------------------------------------------------------------------

// normalizeID 复现 _get_thread_id / _get_run_id:空 id 归一化为 "default"。
func normalizeID(id string) string {
	if id == "" {
		return "default"
	}
	return id
}

// effectiveFreq 返回某工具的有效 (warn, hard) 阈值(优先 per-tool 覆盖)。
func (d *LoopDetector) effectiveFreq(name string) (warn, hard int) {
	if o, ok := d.toolFreqOverrides[name]; ok {
		return o.Warn, o.Hard
	}
	return d.ToolFreqWarn, d.ToolFreqHardLimit
}

// queueWarningLocked 排队一条警告(调用方必须持锁)。对应 _queue_pending_warning。
//   - 去重(同 key 下不重复追加);
//   - 上限 _MAX_PENDING_WARNINGS_PER_RUN(保留最后 4 条);
//   - 触碰 pending key 的 LRU,并修剪超量的 pending key。
func (d *LoopDetector) queueWarningLocked(threadID, runID, warning string) {
	k := pendingKey{threadID, runID}
	warnings := d.pending[k]
	if !containsString(warnings, warning) {
		warnings = append(warnings, warning)
	}
	if len(warnings) > maxPendingWarningsPerRun {
		warnings = warnings[len(warnings)-maxPendingWarningsPerRun:]
	}
	d.pending[k] = warnings
	d.touchPendingKeyLocked(k)
	d.prunePendingLocked(k)
}

// touchThreadLocked 把 thread 移到 LRU 队尾(调用方必须持锁)。
func (d *LoopDetector) touchThreadLocked(threadID string) {
	d.order = removeThreadFromOrder(d.order, threadID)
	d.order = append(d.order, threadID)
}

// evictIfNeededLocked 淘汰最久未用的 thread(调用方必须持锁)。对应 _evict_if_needed。
func (d *LoopDetector) evictIfNeededLocked() {
	for len(d.history) > d.MaxTrackedThreads {
		oldest := d.order[0]
		d.order = d.order[1:]
		delete(d.history, oldest)
		delete(d.warned, oldest)
		delete(d.toolFreq, oldest)
		delete(d.toolFreqWarned, oldest)
		for k := range d.pending {
			if k.threadID == oldest {
				d.dropPendingKeyLocked(k)
			}
		}
	}
}

// dropPendingKeyLocked 删除一个 pending key 的所有簿记(调用方必须持锁)。
func (d *LoopDetector) dropPendingKeyLocked(k pendingKey) {
	delete(d.pending, k)
	d.pendingOrder = removePendingKeyFromOrder(d.pendingOrder, k)
}

// touchPendingKeyLocked 把 pending key 移到 LRU 队尾(调用方必须持锁)。
func (d *LoopDetector) touchPendingKeyLocked(k pendingKey) {
	d.pendingOrder = removePendingKeyFromOrder(d.pendingOrder, k)
	d.pendingOrder = append(d.pendingOrder, k)
}

// prunePendingLocked 修剪超量的 pending key(调用方必须持锁)。对应 _prune_pending_warning_state_locked。
// protected 是「受保护」的 key(当前正在用的),优先淘汰其它 key。
func (d *LoopDetector) prunePendingLocked(protected pendingKey) {
	overflow := len(d.pendingOrder) - d.maxPendingKeys
	if overflow <= 0 {
		return
	}
	candidates := make([]pendingKey, 0, overflow)
	for _, k := range d.pendingOrder {
		if k != protected {
			candidates = append(candidates, k)
		}
	}
	for i := 0; i < overflow && i < len(candidates); i++ {
		d.dropPendingKeyLocked(candidates[i])
	}
}

func removeThreadFromOrder(order []string, id string) []string {
	for i, x := range order {
		if x == id {
			return append(order[:i], order[i+1:]...)
		}
	}
	return order
}

func removePendingKeyFromOrder(order []pendingKey, k pendingKey) []pendingKey {
	for i, x := range order {
		if x == k {
			return append(order[:i], order[i+1:]...)
		}
	}
	return order
}

func containsString(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

// hashToolCalls 对一次调用的 tool_calls 集合做「顺序无关」md5(对应 _hash_tool_calls)。
func hashToolCalls(calls []capability.ToolCall) string {
	normalized := make([]string, 0, len(calls))
	for _, tc := range calls {
		args, fallback := normalizeToolCallArgs(tc.Args)
		key := stableToolKey(tc.Name, args, fallback)
		normalized = append(normalized, tc.Name+":"+key)
	}
	sort.Strings(normalized)
	blob, _ := json.Marshal(normalized)
	sum := md5.Sum(blob)
	return hex.EncodeToString(sum[:])[:12]
}

// normalizeToolCallArgs 复现 _normalize_tool_call_args 的 str 分支。
// Go 的 ToolCall.Args 恒为 JSON 字符串(Python 里的 dict / None / 其它分支不适用):
//   - 空串 → 视为「无参数」({}, nil fallback),对应 Python tc.get("args", {}) 缺省空 dict;
//   - 合法 JSON dict → (dict, nil);
//   - 合法 JSON 非 dict → ({}, 规范 JSON 作为 fallback key);
//   - 非法 JSON → ({}, 原始串作为 fallback key)。
//
// fallback 用 *string 表达:nil = 无 fallback(Python None),非 nil = 有 fallback(即使空串)。
func normalizeToolCallArgs(rawArgs string) (map[string]any, *string) {
	if strings.TrimSpace(rawArgs) == "" {
		return map[string]any{}, nil
	}
	var parsed any
	dec := json.NewDecoder(strings.NewReader(rawArgs))
	dec.UseNumber()
	if err := dec.Decode(&parsed); err != nil {
		return map[string]any{}, StrPtr(rawArgs)
	}
	if m, ok := parsed.(map[string]any); ok {
		return m, nil
	}
	return map[string]any{}, StrPtr(canonicalJSON(parsed))
}

// stableToolKey 复现 _stable_tool_key:从 salient 字段生成稳定 key,避免过拟合噪声。
func stableToolKey(name string, args map[string]any, fallback *string) string {
	// read_file:按 200 行分桶,line 差 1 或行号范围轻微漂移不判为不同调用。
	if name == "read_file" && fallback == nil {
		path := ""
		if p, ok := args["path"]; ok && p != nil {
			if s, ok := p.(string); ok {
				path = s
			} else {
				path = fmt.Sprintf("%v", p)
			}
		}
		startLine := coerceInt(args["start_line"], 1)
		endLine := coerceInt(args["end_line"], startLine)
		if startLine > endLine {
			startLine, endLine = endLine, startLine
		}
		if startLine < 1 {
			startLine = 1
		}
		if endLine < 1 {
			endLine = 1
		}
		const bucketSize = 200
		bucketStart := (startLine - 1) / bucketSize
		bucketEnd := (endLine - 1) / bucketSize
		return fmt.Sprintf("%s:%d-%d", path, bucketStart, bucketEnd)
	}

	// write_file / str_replace:内容敏感,同一 path 迭代中 payload 不同,
	// 只用 salient 字段(path)会把不同调用塌缩成一个,所以哈希全参数降误报。
	if name == "write_file" || name == "str_replace" {
		if fallback != nil {
			return *fallback
		}
		return canonicalJSON(args)
	}

	salient := []string{"path", "url", "query", "command", "pattern", "glob", "cmd"}
	stable := map[string]any{}
	for _, f := range salient {
		if v, ok := args[f]; ok && v != nil {
			stable[f] = v
		}
	}
	if len(stable) > 0 {
		return canonicalJSON(stable)
	}
	if fallback != nil {
		return *fallback
	}
	return canonicalJSON(args)
}

// coerceInt 复现 Python 的 int() 强制转换 + 异常回退默认值。
// 兼容 json.Number(UseNumber 解析)、float64、string、int 等形态。
func coerceInt(v any, def int) int {
	switch t := v.(type) {
	case nil:
		return def
	case json.Number:
		if i, err := strconv.Atoi(t.String()); err == nil {
			return i
		}
		if f, err := t.Float64(); err == nil {
			return int(f)
		}
		return def
	case float64:
		return int(t)
	case int:
		return t
	case int64:
		return int(t)
	case string:
		if i, err := strconv.Atoi(strings.TrimSpace(t)); err == nil {
			return i
		}
		return def
	default:
		return def
	}
}

// canonicalJSON 输出「键排序」的确定性 JSON(对应 json.dumps(sort_keys=True))。
// 保证同一 args 字典永远序列化成同一字符串,是顺序无关哈希的确定性前提。
func canonicalJSON(v any) string {
	var b strings.Builder
	writeCanonicalJSON(&b, v)
	return b.String()
}

func writeCanonicalJSON(b *strings.Builder, v any) {
	switch t := v.(type) {
	case nil:
		b.WriteString("null")
	case bool:
		if t {
			b.WriteString("true")
		} else {
			b.WriteString("false")
		}
	case string:
		enc, _ := json.Marshal(t)
		b.Write(enc)
	case json.Number:
		b.WriteString(t.String())
	case float64:
		enc, _ := json.Marshal(t)
		b.Write(enc)
	case int:
		b.WriteString(strconv.Itoa(t))
	case int64:
		b.WriteString(strconv.FormatInt(t, 10))
	case []any:
		b.WriteByte('[')
		for i, e := range t {
			if i > 0 {
				b.WriteByte(',')
			}
			writeCanonicalJSON(b, e)
		}
		b.WriteByte(']')
	case map[string]any:
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		b.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				b.WriteByte(',')
			}
			ke, _ := json.Marshal(k)
			b.Write(ke)
			b.WriteByte(':')
			writeCanonicalJSON(b, t[k])
		}
		b.WriteByte('}')
	default:
		// 兜底:对应 json.dumps 的 default=str。
		enc, _ := json.Marshal(fmt.Sprintf("%v", t))
		b.Write(enc)
	}
}
