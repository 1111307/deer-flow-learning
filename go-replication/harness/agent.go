// Package harness 是「harness 编排」角色:capability-seam 三件套里的 Consumer。
//
// 它只依赖 capability 包里的接口(ModelProvider / Sandbox / Tool),
// 从不 import 任何具体 provider —— 这正是依赖倒置:编排逻辑与供应商解耦。
//
// 本文件承载两件事:
//  1. turn/step 双层循环的核心结构(Thread / Agent / TurnResult)——
//     对应 deer-flow 的 agents/lead_agent/agent.py(组装 model + tools + middleware)。
//  2. 后台 run 的编排(RunStatus / RunRecord / RunManager)——
//     对应 runtime/runs/manager.py(内存注册表 + 持久化 RunStore + 原子 create_or_reject)。
//
// worker.py::run_agent 驱动 graph.astream 的后台执行逻辑放在 loop.go。
package harness

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"deerflow-go/capability"
)

// Thread 是一次会话的可变状态(对应 deer-flow 的 ThreadState)。
// 消息历史是「唯一的事实来源」,checkpoint/resume 都围绕它做。
type Thread struct {
	ID        string
	Messages  []capability.Message
	SandboxID string // 懒初始化后缓存(对应 ThreadState.sandbox.sandbox_id)
}

// Summarizer 生成摘要。默认用文本拼接;真实实现注入轻量模型(对应 deer-flow 的
// summarization model_name,默认用轻量模型而非主模型)。
type Summarizer func(ctx context.Context, msgs []capability.Message) (string, error)

// TokenCounter 估算一批消息的 token 数(用于 compaction 的 tokens 维度阈值)。
// 默认用「内容字符数 / 4」粗略估算;真实实现注入模型的 tokenizer。
type TokenCounter func(msgs []capability.Message) int

// Agent 是 harness 编排的核心(对应 lead_agent/agent.py::create_agent 的产物)。
// 它同时是 capability-seam 的「Consumer」:消费 ModelProvider / Sandbox / Tool 三类契约。
type Agent struct {
	Model      capability.ModelProvider
	Tools      map[string]capability.Tool
	Sandbox    capability.SandboxProvider
	Middleware []ToolMiddleware

	// Compaction 是长对话压缩配置(见 compaction.go 的 SummarizationConfig)。
	Compaction SummarizationConfig
	Summarizer Summarizer
	// TokenCounter 用于 tokens/fraction 维度的触发与保留阈值。
	TokenCounter TokenCounter
	// MaxInputTokens 是模型最大输入 token(fraction 维度阈值用)。
	MaxInputTokens int
	// BeforeSummarization 是压缩前的钩子(对应 summarization_hook.py 的 memory_flush_hook)。
	BeforeSummarization []BeforeSummarizationHook

	// MaxSteps 是内层 step 上限(对应 deer-flow 的 recursion_limit=100)。
	MaxSteps int
	// LoopDetector 非 nil 时启用循环检测(对应 LoopDetectionMiddleware,见 loopdetect.go)。
	LoopDetector *LoopDetector
	// PatchDangling 启用悬空工具调用补偿(对应 DanglingToolCallMiddleware,见 dangling.go)。
	PatchDangling bool

	// StepObserver 在每个 step 结束后被调用(携带最新消息快照),
	// 供 run_agent 以 stream_mode="values" 发布状态快照。Run/Resume 同步路径下可为 nil。
	StepObserver func(messages []capability.Message)
}

// TurnResult 一次 turn 的结果。
// Interrupt 非 nil 表示整个 run 挂起,等待人工决策 —— 对应 HITL。
type TurnResult struct {
	Reply     string
	Interrupt *capability.InterruptRequest
}

// New 组装一个 Agent(对应 make_lead_agent 的工厂)。
func New(model capability.ModelProvider, tools map[string]capability.Tool, sandbox capability.SandboxProvider) *Agent {
	a := &Agent{
		Model:    model,
		Tools:    tools,
		Sandbox:  sandbox,
		MaxSteps: 100, // 对应 build_run_config 里的 recursion_limit: 100
		Compaction: SummarizationConfig{
			Enabled: false,
		},
	}
	a.Summarizer = a.defaultSummarizer
	a.TokenCounter = defaultTokenCounter
	return a
}

// defaultSummarizer 默认摘要:文本拼接(教学用)。
// 真实实现会注入一个轻量模型,并像 deer-flow 一样要求摘要保留
// 关键决策 / 产物路径 / 未完成 todo,而不是无差别精简。
func (a *Agent) defaultSummarizer(_ context.Context, msgs []capability.Message) (string, error) {
	var b strings.Builder
	for _, m := range msgs {
		if m.Role == "user" || m.Role == "assistant" {
			b.WriteString(m.Role)
			b.WriteString(": ")
			b.WriteString(m.Content)
			b.WriteString("\n")
		}
	}
	return strings.TrimSpace(b.String()), nil
}

// defaultTokenCounter 粗略估算 token 数(内容字符数 / 4,与业界常用估算一致)。
func defaultTokenCounter(msgs []capability.Message) int {
	total := 0
	for _, m := range msgs {
		total += len(m.Content)
		for _, tc := range m.ToolCalls {
			total += len(tc.Args)
		}
	}
	return total / 4
}

// ---------------------------------------------------------------------------
// Run 状态 / 断开模式(对应 runtime/runs/schemas.py)
// ---------------------------------------------------------------------------

// RunStatus 单次 run 的生命周期状态(对应 RunStatus StrEnum)。
type RunStatus string

const (
	RunPending     RunStatus = "pending"
	RunRunning     RunStatus = "running"
	RunSuccess     RunStatus = "success"
	RunError       RunStatus = "error"
	RunTimeout     RunStatus = "timeout"
	RunInterrupted RunStatus = "interrupted"
)

// DisconnectMode SSE 消费者断开时的行为(对应 DisconnectMode StrEnum)。
type DisconnectMode string

const (
	DisconnectCancel   DisconnectMode = "cancel"
	DisconnectContinue DisconnectMode = "continue"
)

// RunRecord 单次 run 的可变记录(对应 manager.py::RunRecord)。
//
// 内存字段(abort event / cancel func / storeOnly)不持久化 —— 对应 Python 里
// asyncio.Event / asyncio.Task / store_only 是进程内存态,持久化只写可序列化子集。
type RunRecord struct {
	RunID             string
	ThreadID          string
	AssistantID       string
	Status            RunStatus
	OnDisconnect      DisconnectMode
	MultitaskStrategy string
	Metadata          map[string]any
	Kwargs            map[string]any
	UserID            string
	CreatedAt         string
	UpdatedAt         string
	ModelName         string
	Err               string

	// AbortAction 记录中断方式:"interrupt"(保留状态)/ "rollback"(回滚到 run 前)。
	AbortAction string

	// 运行时字段(不持久化):
	abortOnce sync.Once
	abortCh   chan struct{}
	cancel    context.CancelFunc
	storeOnly bool
}

// Abort 触发 abort_event(幂等,对应 asyncio.Event.set())。
func (r *RunRecord) Abort() {
	r.abortOnce.Do(func() { close(r.abortCh) })
}

// Aborted 返回是否已触发 abort(非阻塞,对应 asyncio.Event.is_set())。
func (r *RunRecord) Aborted() bool {
	select {
	case <-r.abortCh:
		return true
	default:
		return false
	}
}

// Cancel 取消关联的后台任务(对应 task.cancel())。无任务时是 no-op。
func (r *RunRecord) Cancel() {
	if r.cancel != nil {
		r.cancel()
	}
}

func newRunRecord(runID, threadID, assistantID string, onDisconnect DisconnectMode, strategy, modelName, userID string, metadata, kwargs map[string]any) *RunRecord {
	return &RunRecord{
		RunID:             runID,
		ThreadID:          threadID,
		AssistantID:       assistantID,
		Status:            RunPending,
		OnDisconnect:      onDisconnect,
		MultitaskStrategy: strategy,
		Metadata:          metadata,
		Kwargs:            kwargs,
		UserID:            userID,
		ModelName:         modelName,
		CreatedAt:         nowISO(),
		UpdatedAt:         nowISO(),
		AbortAction:       "interrupt",
		abortCh:           make(chan struct{}),
	}
}

func nowISO() string {
	return time.Now().UTC().Format(time.RFC3339Nano)
}

// RunStore 是可选的后台持久化(对应 runtime/runs/store/base.py::RunStore)。
// 内存注册表保证进程内一致;RunStore 让 run 历史跨进程重启存活。
type RunStore interface {
	Put(runID string, rec *RunRecord) error
	Get(runID string) (*RunRecord, bool, error)
	ListByThread(threadID string, limit int) ([]*RunRecord, error)
	UpdateStatus(runID string, status RunStatus, errMsg string) (bool, error)
	ListInflight() ([]*RunRecord, error)
}

// ConflictError 对应 manager.py::ConflictError:multitask_strategy=reject 且已有 inflight run。
type ConflictError struct{ ThreadID string }

func (e *ConflictError) Error() string {
	return fmt.Sprintf("Thread %s already has an active run", e.ThreadID)
}

// UnsupportedStrategyError 对应 manager.py::UnsupportedStrategyError。
type UnsupportedStrategyError struct{ Strategy string }

func (e *UnsupportedStrategyError) Error() string {
	return fmt.Sprintf("Multitask strategy '%s' is not yet supported. Supported strategies: reject, interrupt, rollback", e.Strategy)
}

var supportedStrategies = map[string]bool{"reject": true, "interrupt": true, "rollback": true}

// RunManager 是进程内 run 注册表 + 可选持久化 RunStore。
// 对应 manager.py::RunManager:所有变更用 mutex 串行化;提供 store 时同步持久化
// 可序列化子集,使 run 历史跨进程重启存活。
type RunManager struct {
	mu           sync.Mutex
	runs         map[string]*RunRecord
	runsByThread map[string]map[string]struct{} // thread_id -> run_id 有序集合
	store        RunStore
}

// NewRunManager 构造 RunManager。store 为 nil 表示纯内存(不持久化)。
func NewRunManager(store RunStore) *RunManager {
	return &RunManager{
		runs:         map[string]*RunRecord{},
		runsByThread: map[string]map[string]struct{}{},
		store:        store,
	}
}

func (m *RunManager) indexRunLocked(rec *RunRecord) {
	bucket := m.runsByThread[rec.ThreadID]
	if bucket == nil {
		bucket = map[string]struct{}{}
		m.runsByThread[rec.ThreadID] = bucket
	}
	bucket[rec.RunID] = struct{}{}
}

func (m *RunManager) unindexRunLocked(runID, threadID string) {
	bucket := m.runsByThread[threadID]
	if bucket != nil {
		delete(bucket, runID)
		if len(bucket) == 0 {
			delete(m.runsByThread, threadID)
		}
	}
}

// threadRecordsLocked 返回某 thread 的存活内存记录(调用方必须持锁)。
func (m *RunManager) threadRecordsLocked(threadID string) []*RunRecord {
	ids := m.runsByThread[threadID]
	if len(ids) == 0 {
		return nil
	}
	out := make([]*RunRecord, 0, len(ids))
	for runID := range ids {
		if rec := m.runs[runID]; rec != nil {
			out = append(out, rec)
		}
	}
	return out
}

// isInflight 判断状态是否 pending/running。
func isInflight(s RunStatus) bool { return s == RunPending || s == RunRunning }

// Get 按 id 取回记录:先查内存,再回退到持久化 store。
func (m *RunManager) Get(runID string) *RunRecord {
	m.mu.Lock()
	if rec := m.runs[runID]; rec != nil {
		m.mu.Unlock()
		return rec
	}
	m.mu.Unlock()

	if m.store == nil {
		return nil
	}
	rec, ok, _ := m.store.Get(runID)
	if !ok {
		return nil
	}
	// 双检:store 调用期间可能并发 create() 插入了内存记录。
	m.mu.Lock()
	if live := m.runs[runID]; live != nil {
		m.mu.Unlock()
		return live
	}
	m.mu.Unlock()
	rec.storeOnly = true
	return rec
}

// HasInflight 返回 thread 是否有 pending/running 的 run。
func (m *RunManager) HasInflight(threadID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, r := range m.threadRecordsLocked(threadID) {
		if isInflight(r.Status) {
			return true
		}
	}
	return false
}

// SetStatus 迁移 run 状态并(尽力)持久化。
func (m *RunManager) SetStatus(runID string, status RunStatus, errMsg string) {
	m.mu.Lock()
	rec := m.runs[runID]
	if rec == nil {
		m.mu.Unlock()
		return
	}
	rec.Status = status
	rec.UpdatedAt = nowISO()
	if errMsg != "" {
		rec.Err = errMsg
	}
	m.mu.Unlock()

	if m.store != nil {
		_, _ = m.store.UpdateStatus(runID, status, errMsg)
	}
}

// Create 创建一个 pending run 并注册(持久化失败时回滚内存记录)。
func (m *RunManager) Create(threadID, assistantID string, onDisconnect DisconnectMode, strategy, modelName, userID string, metadata, kwargs map[string]any) (*RunRecord, error) {
	rec := newRunRecord(newRunID(), threadID, assistantID, onDisconnect, strategy, modelName, userID, metadata, kwargs)
	m.mu.Lock()
	m.runs[rec.RunID] = rec
	m.indexRunLocked(rec)
	persisted := false
	if m.store != nil {
		if err := m.store.Put(rec.RunID, rec); err != nil {
			// 回滚内存记录(对应 manager.py create 的 finally 回滚)。
			delete(m.runs, rec.RunID)
			m.unindexRunLocked(rec.RunID, rec.ThreadID)
			m.mu.Unlock()
			return nil, err
		}
		persisted = true
	}
	_ = persisted
	m.mu.Unlock()
	return rec, nil
}

// CreateOrReject 原子地「检查 inflight + 创建」,消除 TOCTOU 竞态。
// 对应 manager.py::create_or_reject:
//   - reject:已有 pending/running run 则返回 ConflictError。
//   - interrupt/rollback:先取消 inflight run(abort_event.set + task.cancel),
//     再创建新 run。
//
// 持锁横跨「检查 + 插入」,对应 Python 里 async with self._lock 包住 check + insert。
func (m *RunManager) CreateOrReject(threadID, assistantID string, onDisconnect DisconnectMode, strategy, modelName, userID string, metadata, kwargs map[string]any) (*RunRecord, error) {
	if !supportedStrategies[strategy] {
		return nil, &UnsupportedStrategyError{Strategy: strategy}
	}

	m.mu.Lock()
	inflight := []*RunRecord{}
	for _, r := range m.threadRecordsLocked(threadID) {
		if isInflight(r.Status) {
			inflight = append(inflight, r)
		}
	}
	if strategy == "reject" && len(inflight) > 0 {
		m.mu.Unlock()
		return nil, &ConflictError{ThreadID: threadID}
	}

	rec := newRunRecord(newRunID(), threadID, assistantID, onDisconnect, strategy, modelName, userID, metadata, kwargs)
	m.runs[rec.RunID] = rec
	m.indexRunLocked(rec)
	if m.store != nil {
		if err := m.store.Put(rec.RunID, rec); err != nil {
			delete(m.runs, rec.RunID)
			m.unindexRunLocked(rec.RunID, rec.ThreadID)
			m.mu.Unlock()
			return nil, err
		}
	}

	// interrupt/rollback:取消 inflight run。
	interrupted := []*RunRecord{}
	if strategy == "interrupt" || strategy == "rollback" {
		for _, r := range inflight {
			r.AbortAction = strategy
			r.Abort()
			r.Cancel()
			r.Status = RunInterrupted
			r.UpdatedAt = nowISO()
			interrupted = append(interrupted, r)
		}
	}
	m.mu.Unlock()

	for _, r := range interrupted {
		if m.store != nil {
			_, _ = m.store.UpdateStatus(r.RunID, RunInterrupted, "")
		}
	}
	return rec, nil
}

// Cancel 请求取消 run(幂等:二次 cancel 是 no-op 成功)。
// 对应 manager.py::cancel:abort_event.set + task.cancel;已 interrupted 返回 true,
// 已到终态(非 interrupted)返回 false,未知 run 返回 false。
func (m *RunManager) Cancel(runID, action string) bool {
	m.mu.Lock()
	rec := m.runs[runID]
	if rec == nil {
		m.mu.Unlock()
		return false
	}
	if rec.Status == RunInterrupted {
		m.mu.Unlock()
		return true // 幂等
	}
	if !isInflight(rec.Status) {
		m.mu.Unlock()
		return false
	}
	rec.AbortAction = action
	rec.Abort()
	rec.Cancel()
	rec.Status = RunInterrupted
	rec.UpdatedAt = nowISO()
	m.mu.Unlock()
	if m.store != nil {
		_, _ = m.store.UpdateStatus(runID, RunInterrupted, "")
	}
	return true
}

// ReconcileOrphanedInflightRuns 把持久化里「无本地 task 接管」的 active run 标记为 error。
// 对应 manager.py::reconcile_orphaned_inflight_runs:SQLite 重启后,任何进程重启前
// 创建的 pending/running 行都不可能有本地 worker,这一步把歧义状态转成显式错误。
func (m *RunManager) ReconcileOrphanedInflightRuns(errMsg string) []*RunRecord {
	if m.store == nil {
		return nil
	}
	rows, err := m.store.ListInflight()
	if err != nil {
		return nil
	}
	var recovered []*RunRecord
	for _, row := range rows {
		row.storeOnly = true
		m.mu.Lock()
		live := m.runs[row.RunID]
		if live != nil && isInflight(live.Status) {
			m.mu.Unlock()
			continue // 有本地 worker 接管,跳过。
		}
		m.mu.Unlock()

		row.Status = RunError
		row.Err = errMsg
		row.UpdatedAt = nowISO()
		if ok, _ := m.store.UpdateStatus(row.RunID, RunError, errMsg); ok {
			recovered = append(recovered, row)
		}
	}
	return recovered
}

// ListByThread 返回 thread 的 run(内存优先,合并 store,按创建时间倒序,最多 limit 条)。
func (m *RunManager) ListByThread(threadID string, limit int) []*RunRecord {
	m.mu.Lock()
	memory := m.threadRecordsLocked(threadID)
	m.mu.Unlock()

	byID := map[string]*RunRecord{}
	for _, r := range memory {
		byID[r.RunID] = r
	}
	if m.store != nil {
		storeLimit := limit - len(memory)
		if storeLimit > 0 {
			if rows, err := m.store.ListByThread(threadID, storeLimit); err == nil {
				for _, row := range rows {
					if _, ok := byID[row.RunID]; !ok {
						row.storeOnly = true
						byID[row.RunID] = row
					}
				}
			}
		}
	}
	out := make([]*RunRecord, 0, len(byID))
	for _, r := range byID {
		out = append(out, r)
	}
	// 倒序 by CreatedAt。
	sortRunsNewestFirst(out)
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out
}

func sortRunsNewestFirst(recs []*RunRecord) {
	for i := 1; i < len(recs); i++ {
		for j := i; j > 0 && recs[j].CreatedAt > recs[j-1].CreatedAt; j-- {
			recs[j], recs[j-1] = recs[j-1], recs[j]
		}
	}
}

// Shutdown 取消所有 inflight run(对应 manager.py::shutdown 的 drain 语义简化版)。
// Go 里每个 run 由 goroutine + context 驱动,cancel 即取消;run 自行 settle 后保留真实终态。
func (m *RunManager) Shutdown() {
	m.mu.Lock()
	var inflight []*RunRecord
	for _, r := range m.runs {
		if isInflight(r.Status) {
			inflight = append(inflight, r)
		}
	}
	for _, r := range inflight {
		r.AbortAction = "interrupt"
		r.Abort()
		r.Cancel()
	}
	m.mu.Unlock()
}

func newRunID() string {
	return fmt.Sprintf("run-%d", time.Now().UnixNano())
}
