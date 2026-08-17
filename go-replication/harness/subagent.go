// Sub-Agent 委派(多智能体 + 并发限流)—— 对应 deer-flow 六个源文件:
//
//   - subagents/executor.py::SubagentExecutor(双线程池调度/执行 + 超时 + 结果结构化)
//   - subagents/registry.py(内置 + 自定义 + per-agent 覆盖的解析)
//   - subagents/config.py::SubagentConfig(名字/系统提示/工具/模型/回合/超时)
//   - subagents/builtins/general_purpose.py + bash_agent.py(内置子 agent)
//   - tools/builtins/task_tool.py::task_tool(委派 + 后台执行 + 轮询 + SSE 事件)
//   - agents/middlewares/subagent_limit_middleware.py(截断超额 task 调用)
//
// 核心设计(与 deer-flow 一致):
//   - 上下文隔离:子 agent 只拿到 description/prompt + 自己的 system prompt,
//     看不到主 agent 的对话历史(独立 ThreadState)。
//   - 并发限流:MAX_CONCURRENT_SUBAGENTS=3。两层:
//   - SubagentLimitMiddleware 把单次模型响应里超额的 task 调用截断(clamp [2,4]);
//   - 执行器用带缓冲 channel 做信号量,最多 3 个后台执行并发。
//   - 一次性执行:checkpointer=False,子 agent 不背 checkpoint,不可 resume。
//   - 结构化返回:SubagentResult 带 status/result/error/ai_messages/token_usage,
//     主 agent 拿到的不是完整 transcript。
//
// 与 Python 的关键差异:
//   - 双线程池(scheduler ThreadPoolExecutor(3) + 常驻 isolated event loop)在 Go 里
//     坍缩成 goroutine + 带缓冲 channel 信号量:Python 需要 isolated loop 是因为 anyio
//     的「cancel scope 必须在进入它的同一任务退出」;Go 的 goroutine 没有这个约束。
//   - threading.Event(协作取消)-> close(chan struct{})。
//   - future.result(timeout=...) -> context.WithTimeout + errors.Is(err, DeadlineExceeded)。
//   - LangGraph astream(stream_mode="values")+ 逐 chunk 采集 AI 消息 -> SubagentRunner
//     接口 + emit 回调(每个中间状态回调一次消息列表)。
//   - get_stream_writer() 的 SSE 事件 -> SubagentEventSink 回调。
package harness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"deerflow-go/capability"
)

// ── 状态与结果 ───────────────────────────────────────────────────────────────

// SubagentStatus 是子 agent 执行状态(对应 SubagentStatus Enum)。
type SubagentStatus string

const (
	SubagentPending   SubagentStatus = "pending"
	SubagentRunning   SubagentStatus = "running"
	SubagentCompleted SubagentStatus = "completed"
	SubagentFailed    SubagentStatus = "failed"
	SubagentCancelled SubagentStatus = "cancelled"
	SubagentTimedOut  SubagentStatus = "timed_out"
)

// IsTerminal 判断是否终态(对应 is_terminal)。
func (s SubagentStatus) IsTerminal() bool {
	switch s {
	case SubagentCompleted, SubagentFailed, SubagentCancelled, SubagentTimedOut:
		return true
	default:
		return false
	}
}

// TokenUsageRecord 是子 agent 一次 LLM 调用的 token 用量(对应 SubagentTokenCollector 的 records)。
type TokenUsageRecord struct {
	SourceRunID  string `json:"source_run_id"`
	Caller       string `json:"caller"`
	ModelName    string `json:"model_name"`
	InputTokens  int    `json:"input_tokens"`
	OutputTokens int    `json:"output_tokens"`
	TotalTokens  int    `json:"total_tokens"`
}

// SubagentResult 是一次子 agent 执行的结果(对应 SubagentResult dataclass)。
type SubagentResult struct {
	mu                sync.Mutex
	TaskID            string
	TraceID           string
	Status            SubagentStatus
	Result            string
	Error             string
	StartedAt         time.Time
	CompletedAt       time.Time
	AIMessages        []capability.Message
	TokenUsageRecords []TokenUsageRecord
	UsageReported     bool
	cancel            chan struct{}
	cancelOnce        sync.Once
}

// NewSubagentResult 构造结果对象,并初始化协作取消通道。
func NewSubagentResult(taskID, traceID string, status SubagentStatus) *SubagentResult {
	return &SubagentResult{
		TaskID:  taskID,
		TraceID: traceID,
		Status:  status,
		cancel:  make(chan struct{}),
	}
}

// RequestCancel 请求协作取消(对应 cancel_event.set())。
// 幂等:多次调用只 close 一次。
func (r *SubagentResult) RequestCancel() {
	r.cancelOnce.Do(func() { close(r.cancel) })
}

// CancelRequested 是否已请求取消(对应 cancel_event.is_set())。
func (r *SubagentResult) CancelRequested() bool {
	select {
	case <-r.cancel:
		return true
	default:
		return false
	}
}

// TrySetTerminal 一次性设置终态(对应 try_set_terminal)。
// 后台超时/取消与执行 worker 可能竞争同一个 holder;第一个终态转换生效,
// 迟到的终态写入不得改变 status 或 payload。返回是否成功设置。
func (r *SubagentResult) TrySetTerminal(status SubagentStatus, result, errMsg string, completedAt time.Time, aiMessages []capability.Message, tokens []TokenUsageRecord) bool {
	if !status.IsTerminal() {
		return false // 对应 raise ValueError("Status ... is not terminal")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.Status.IsTerminal() {
		return false
	}
	if result != "" {
		r.Result = result
	}
	if errMsg != "" {
		r.Error = errMsg
	}
	if aiMessages != nil {
		r.AIMessages = aiMessages
	}
	if tokens != nil {
		r.TokenUsageRecords = tokens
	}
	if completedAt.IsZero() {
		completedAt = time.Now()
	}
	r.CompletedAt = completedAt
	r.Status = status
	return true
}

// SetRunning 把结果置为 RUNNING 并记录开始时间(对应 execute_async 的 run_task)。
func (r *SubagentResult) SetRunning(now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Status = SubagentRunning
	r.StartedAt = now
}

// StatusValue 线程安全读取状态。
func (r *SubagentResult) StatusValue() SubagentStatus {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.Status
}

// ResultValue 线程安全读取结果文本。
func (r *SubagentResult) ResultValue() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.Result
}

// ErrorValue 线程安全读取错误。
func (r *SubagentResult) ErrorValue() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.Error
}

// IsTerminalValue 线程安全判断是否终态。
func (r *SubagentResult) IsTerminalValue() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.Status.IsTerminal()
}

// AIMessagesSnapshot 返回 AI 消息快照。
func (r *SubagentResult) AIMessagesSnapshot() []capability.Message {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]capability.Message(nil), r.AIMessages...)
}

// TokenUsageSnapshot 返回 token 用量快照。
func (r *SubagentResult) TokenUsageSnapshot() []TokenUsageRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]TokenUsageRecord(nil), r.TokenUsageRecords...)
}

// SummarizeUsage 汇总 token 用量(对应 _summarize_usage)。
func SummarizeUsage(records []TokenUsageRecord) (map[string]int, bool) {
	if len(records) == 0 {
		return nil, false
	}
	out := map[string]int{"input_tokens": 0, "output_tokens": 0, "total_tokens": 0}
	for _, rec := range records {
		out["input_tokens"] += rec.InputTokens
		out["output_tokens"] += rec.OutputTokens
		out["total_tokens"] += rec.TotalTokens
	}
	return out, true
}

// ── 后台任务注册表(对应 _background_tasks)───────────────────────────────────

var (
	backgroundTasksMu sync.Mutex
	backgroundTasks   = map[string]*SubagentResult{}
)

// GetBackgroundTaskResult 获取后台任务结果(对应 get_background_task_result)。
func GetBackgroundTaskResult(taskID string) *SubagentResult {
	backgroundTasksMu.Lock()
	defer backgroundTasksMu.Unlock()
	return backgroundTasks[taskID]
}

// ListBackgroundTasks 列出所有后台任务(对应 list_background_tasks)。
func ListBackgroundTasks() []*SubagentResult {
	backgroundTasksMu.Lock()
	defer backgroundTasksMu.Unlock()
	out := make([]*SubagentResult, 0, len(backgroundTasks))
	for _, r := range backgroundTasks {
		out = append(out, r)
	}
	return out
}

// RequestCancelBackgroundTask 请求取消后台任务(对应 request_cancel_background_task)。
func RequestCancelBackgroundTask(taskID string) {
	backgroundTasksMu.Lock()
	result := backgroundTasks[taskID]
	backgroundTasksMu.Unlock()
	if result != nil {
		result.RequestCancel()
	}
}

// CleanupBackgroundTask 移除终态任务,防止内存泄漏(对应 cleanup_background_task)。
// 只移除终态任务,避免与仍在更新的后台执行器竞争。
func CleanupBackgroundTask(taskID string) {
	backgroundTasksMu.Lock()
	defer backgroundTasksMu.Unlock()
	result := backgroundTasks[taskID]
	if result == nil {
		return
	}
	if result.IsTerminalValue() || !result.CompletedAt.IsZero() {
		delete(backgroundTasks, taskID)
	}
}

// ── 配置与内置子 agent ──────────────────────────────────────────────────────

// MaxConcurrentSubagents 并发子 agent 上限(对应 MAX_CONCURRENT_SUBAGENTS=3)。
const MaxConcurrentSubagents = 3

// DefaultSubagentTimeoutSeconds 内置子 agent 的全局超时默认值(30 分钟)。
// 对应 config.py 文档里「built-in 的 effective limit 是全局 subagents.timeout_seconds(默认 1800)」。
const DefaultSubagentTimeoutSeconds = 1800

// SubagentPollInterval task_tool 的轮询间隔(对应 asyncio.sleep(5))。
const SubagentPollInterval = 5 * time.Second

// SubagentConfig 是子 agent 配置(对应 config.py::SubagentConfig)。
type SubagentConfig struct {
	Name            string
	Description     string
	SystemPrompt    string
	Tools           []string // nil = 继承所有工具
	DisallowedTools []string // 默认 ["task"]
	Skills          []string // nil = 继承所有 enabled skills;[] = 无 skills
	Model           string   // "inherit" = 用父模型
	MaxTurns        int      // 默认 50
	TimeoutSeconds  int      // 默认 900(裸兜底;内置由全局 1800 覆盖)
}

// LocalBashSubagentDisabledMessage 对应 security.py::LOCAL_BASH_SUBAGENT_DISABLED_MESSAGE。
const LocalBashSubagentDisabledMessage = "Bash subagent is disabled for LocalSandboxProvider because host bash execution is not a secure sandbox boundary. Switch to AioSandboxProvider for isolated bash access, or set sandbox.allow_host_bash: true only in a fully trusted local environment."

// GeneralPurposeConfig 对应 GENERAL_PURPOSE_CONFIG。
var GeneralPurposeConfig = SubagentConfig{
	Name:            "general-purpose",
	Model:           "inherit",
	MaxTurns:        150,
	TimeoutSeconds:  900, // 裸兜底;内置由全局 1800 覆盖(见 GetSubagentConfig)
	Tools:           nil,
	DisallowedTools: []string{"task", "ask_clarification", "present_files"},
	Description: `A capable agent for complex, multi-step tasks that require both exploration and action.

Use this subagent when:
- The task requires both exploration and modification
- Complex reasoning is needed to interpret results
- Multiple dependent steps must be executed
- The task would benefit from isolated context management

Do NOT use for simple, single-step operations.`,
	SystemPrompt: `You are a general-purpose subagent working on a delegated task. Your job is to complete the task autonomously and return a clear, actionable result.

<guidelines>
- Focus on completing the delegated task efficiently
- Use available tools as needed to accomplish the goal
- Think step by step but act decisively
- If you encounter issues, explain them clearly in your response
- Return a concise summary of what you accomplished
- Do NOT ask for clarification - work with the information provided
</guidelines>

<output_format>
When you complete the task, provide:
1. A brief summary of what was accomplished
2. Key findings or results
3. Any relevant file paths, data, or artifacts created
4. Issues encountered (if any)
</output_format>`,
}

// BashAgentConfig 对应 BASH_AGENT_CONFIG。
var BashAgentConfig = SubagentConfig{
	Name:            "bash",
	Model:           "inherit",
	MaxTurns:        60,
	TimeoutSeconds:  900, // 裸兜底;内置由全局 1800 覆盖(见 GetSubagentConfig)
	Tools:           []string{"bash", "ls", "read_file", "write_file", "str_replace"},
	DisallowedTools: []string{"task", "ask_clarification", "present_files"},
	Description: `Command execution specialist for running bash commands in a separate context.

Use this subagent when:
- You need to run a series of related bash commands
- Terminal operations like git, npm, docker, etc.
- Command output is verbose and would clutter main context
- Build, test, or deployment operations

Do NOT use for simple single commands - use bash tool directly instead.`,
	SystemPrompt: `You are a bash command execution specialist. Execute the requested commands carefully and report results clearly.

<guidelines>
- Execute commands one at a time when they depend on each other
- Use parallel execution when commands are independent
- Report both stdout and stderr when relevant
- Handle errors gracefully and explain what went wrong
- Be cautious with destructive operations (rm, overwrite, etc.)
</guidelines>

<output_format>
For each command or group of commands:
1. What was executed
2. The result (success/failure)
3. Relevant output (summarized if verbose)
4. Any errors or warnings
</output_format>`,
}

// BuiltinSubagents 是内置子 agent 注册表(对应 BUILTIN_SUBAGENTS)。
var BuiltinSubagents = map[string]SubagentConfig{
	"general-purpose": GeneralPurposeConfig,
	"bash":            BashAgentConfig,
}

// ResolveSubagentModelName 解析子 agent 的有效模型名(对应 resolve_subagent_model_name)。
// 返回空串表示「没有可解析的默认模型」(harness 不持有模型注册表,由 provider 层提供)。
func ResolveSubagentModelName(config SubagentConfig, parentModel string) string {
	if config.Model != "inherit" {
		return config.Model
	}
	if parentModel != "" {
		return parentModel
	}
	return ""
}

// SubagentOverride 是 per-agent 覆盖(对应 registry 里 agent_override)。
// 指针 nil = 未设置该字段。
type SubagentOverride struct {
	TimeoutSeconds *int
	MaxTurns       *int
	Model          *string
	Skills         *[]string
}

// SubagentsAppConfig 对应 config.yaml 的 subagents 段:自定义 agent + 全局默认 + per-agent 覆盖。
type SubagentsAppConfig struct {
	CustomAgents   map[string]SubagentConfig
	Agents         map[string]SubagentOverride
	TimeoutSeconds int // 全局默认(内置专用,默认 1800)
	MaxTurns       int // 全局默认(0 = 未设置)
}

// cloneConfig 浅拷贝一份配置(对应 dataclasses.replace)。
func cloneConfig(c SubagentConfig) *SubagentConfig {
	cp := c
	cp.Tools = append([]string(nil), c.Tools...)
	cp.DisallowedTools = append([]string(nil), c.DisallowedTools...)
	cp.Skills = append([]string(nil), c.Skills...)
	return &cp
}

// GetSubagentConfig 按名字解析子 agent 配置(对应 get_subagent_config)。
// 解析顺序:内置 -> 自定义 -> per-agent 覆盖。找不到返回 nil。
func GetSubagentConfig(name string, cfg *SubagentsAppConfig) *SubagentConfig {
	config, isBuiltin := BuiltinSubagents[name]
	if !isBuiltin {
		if cfg != nil {
			if c, ok := cfg.CustomAgents[name]; ok {
				config = c
			} else {
				return nil
			}
		} else {
			return nil
		}
	}

	result := cloneConfig(config)

	// per-agent 覆盖(对应 agents 段)。
	var override *SubagentOverride
	if cfg != nil {
		if o, ok := cfg.Agents[name]; ok {
			override = &o
		}
	}

	// Timeout:per-agent 覆盖 > 全局默认(仅内置)> 配置自身值。
	if override != nil && override.TimeoutSeconds != nil {
		result.TimeoutSeconds = *override.TimeoutSeconds
	} else if isBuiltin && cfg != nil && cfg.TimeoutSeconds != 0 && cfg.TimeoutSeconds != result.TimeoutSeconds {
		result.TimeoutSeconds = cfg.TimeoutSeconds
	}

	// Max turns:per-agent 覆盖 > 全局默认(仅内置)> 配置自身值。
	if override != nil && override.MaxTurns != nil {
		result.MaxTurns = *override.MaxTurns
	} else if isBuiltin && cfg != nil && cfg.MaxTurns != 0 && cfg.MaxTurns != result.MaxTurns {
		result.MaxTurns = cfg.MaxTurns
	}

	// Model / Skills:仅 per-agent 覆盖(无全局默认)。
	if override != nil && override.Model != nil {
		result.Model = *override.Model
	}
	if override != nil && override.Skills != nil {
		result.Skills = append([]string(nil), (*override.Skills)...)
	}

	return result
}

// GetSubagentNames 列出所有可用子 agent 名字(内置 + 自定义)。
func GetSubagentNames(cfg *SubagentsAppConfig) []string {
	names := make([]string, 0, len(BuiltinSubagents))
	for n := range BuiltinSubagents {
		names = append(names, n)
	}
	if cfg != nil {
		for n := range cfg.CustomAgents {
			if !containsString(names, n) {
				names = append(names, n)
			}
		}
	}
	return names
}

// GetAvailableSubagentNames 返回当前沙盒配置下可暴露的子 agent 名。
// bash 仅在 host bash 允许时暴露(对应 get_available_subagent_names)。
func GetAvailableSubagentNames(cfg *SubagentsAppConfig, hostBashAllowed bool) []string {
	names := GetSubagentNames(cfg)
	if !hostBashAllowed {
		out := names[:0]
		for _, n := range names {
			if n != "bash" {
				out = append(out, n)
			}
		}
		return out
	}
	return names
}

// ListSubagents 列出所有解析后的子 agent 配置。
func ListSubagents(cfg *SubagentsAppConfig) []*SubagentConfig {
	var out []*SubagentConfig
	for _, n := range GetSubagentNames(cfg) {
		if c := GetSubagentConfig(n, cfg); c != nil {
			out = append(out, c)
		}
	}
	return out
}

// ── 运行抽象 ─────────────────────────────────────────────────────────────────

// SubagentRun 是一次子 agent 运行的隔离上下文。子 agent 只看到这些,
// 看不到主 agent 历史 —— 这就是「上下文隔离」。
type SubagentRun struct {
	Name          string
	SystemPrompt  string
	Task          string
	Tools         []capability.Tool
	MaxTurns      int
	ThreadID      string
	DeferredSetup *capability.DeferredToolSetup // nil = 无延迟工具
}

// SubagentRunResult 是一次子 agent 运行的完整产出。
type SubagentRunResult struct {
	Result            string
	TokenUsageRecords []TokenUsageRecord
}

// SubagentRunner 是「跑一个子 agent」的核心抽象,对应 create_agent(...).astream(...)。
// 以流式方式产出中间状态:每产生一步就调用 emit(messages) 让执行器采集 AI 消息
// (用于 task_running SSE 事件);返回最终结构化结果。emit 可能为 nil(同步简单路径)。
type SubagentRunner func(ctx context.Context, run SubagentRun, emit func(messages []capability.Message)) (SubagentRunResult, error)

// SubagentTask 是委派给子 agent 的任务(对应 task_tool 的入参,简单路径用)。
type SubagentTask struct {
	Description string
	Prompt      string
	Type        string
}

// adaptSimpleRunner 把简单 run func 适配成 SubagentRunner(不流式、不采集 AI 消息)。
func adaptSimpleRunner(run func(context.Context, SubagentTask) (string, error)) SubagentRunner {
	return func(ctx context.Context, r SubagentRun, _ func([]capability.Message)) (SubagentRunResult, error) {
		s, err := run(ctx, SubagentTask{Description: r.Name, Prompt: r.Task, Type: r.Name})
		return SubagentRunResult{Result: s}, err
	}
}

// ── 执行器 ───────────────────────────────────────────────────────────────────

// SubagentExecutor 在独立上下文运行子 agent,并做并发限流。
type SubagentExecutor struct {
	config            SubagentConfig
	baseTools         []capability.Tool
	runner            SubagentRunner
	sem               chan struct{} // 带缓冲 channel = 信号量(对应 MAX_CONCURRENT_SUBAGENTS)
	traceID           string
	userID            string
	threadID          string
	skillToolFilter   func([]capability.Tool) []capability.Tool // nil = 恒等(对应 skill allowed tools 过滤)
	toolSearchEnabled bool
}

// NewSubagentExecutor 构造简单执行器(演示/测试兼容)。
// maxConcurrent 是并发上限(对应 =3);run 是同步执行函数。
func NewSubagentExecutor(maxConcurrent int, run func(context.Context, SubagentTask) (string, error)) *SubagentExecutor {
	return NewSubagentExecutorWithConfig(GeneralPurposeConfig, nil, adaptSimpleRunner(run), maxConcurrent)
}

// NewSubagentExecutorWithConfig 构造完整执行器。
// tools 会被 config.Tools allowlist + config.DisallowedTools denylist 过滤(对应 _filter_tools)。
func NewSubagentExecutorWithConfig(cfg SubagentConfig, tools []capability.Tool, runner SubagentRunner, maxConcurrent int) *SubagentExecutor {
	if maxConcurrent < 1 {
		maxConcurrent = 1
	}
	if runner == nil {
		runner = func(_ context.Context, _ SubagentRun, _ func([]capability.Message)) (SubagentRunResult, error) {
			return SubagentRunResult{Result: "No response generated"}, nil
		}
	}
	baseTools := filterSubagentTools(tools, cfg.Tools, cfg.DisallowedTools)
	return &SubagentExecutor{
		config:    cfg,
		baseTools: baseTools,
		runner:    runner,
		sem:       make(chan struct{}, maxConcurrent),
		traceID:   randomHex(8),
	}
}

// SetThreadID 设置线程 ID(沙盒操作与追踪用,对应 thread_id)。
func (e *SubagentExecutor) SetThreadID(id string) { e.threadID = id }

// SetUserID 设置用户 ID(追踪用,对应 user_id)。
func (e *SubagentExecutor) SetUserID(id string) { e.userID = id }

// SetTraceID 设置 trace ID(对应 trace_id)。
func (e *SubagentExecutor) SetTraceID(id string) { e.traceID = id }

// SetSkillToolFilter 注入 skill allowed-tools 过滤(对应 _apply_skill_allowed_tools)。
func (e *SubagentExecutor) SetSkillToolFilter(f func([]capability.Tool) []capability.Tool) {
	e.skillToolFilter = f
}

// SetToolSearchEnabled 开关延迟工具绑定(对应 tool_search.enabled)。
func (e *SubagentExecutor) SetToolSearchEnabled(enabled bool) { e.toolSearchEnabled = enabled }

// filterSubagentTools 按 allowlist + denylist 过滤工具(对应 _filter_tools)。
func filterSubagentTools(all []capability.Tool, allowed, disallowed []string) []capability.Tool {
	filtered := all
	if allowed != nil {
		allowedSet := stringSet(allowed)
		out := filtered[:0]
		for _, t := range filtered {
			if allowedSet[t.Name()] {
				out = append(out, t)
			}
		}
		filtered = out
	}
	if disallowed != nil {
		disallowedSet := stringSet(disallowed)
		out := filtered[:0]
		for _, t := range filtered {
			if !disallowedSet[t.Name()] {
				out = append(out, t)
			}
		}
		filtered = out
	}
	return filtered
}

// buildInitialState 构建子 agent 初始状态(对应 _build_initial_state):
// skill 过滤 -> 延迟工具组装 -> 拼接 system prompt(含 <available-deferred-tools>)。
func (e *SubagentExecutor) buildInitialState(task string) (SubagentRun, error) {
	filteredTools := e.baseTools
	if e.skillToolFilter != nil {
		filteredTools = e.skillToolFilter(e.baseTools)
	}

	systemPrompt := e.config.SystemPrompt
	var deferredSetup *capability.DeferredToolSetup
	finalTools := filteredTools
	if e.toolSearchEnabled {
		ft, setup, err := capability.AssembleDeferredTools(filteredTools, true)
		if err != nil {
			// fail-closed:不绑定完整 MCP schema,直接失败(与 lead 路径一致)。
			return SubagentRun{}, err
		}
		finalTools = ft
		deferredSetup = setup
		if section := capability.GetDeferredToolsPromptSection(setup.DeferredNames); section != "" {
			systemPrompt = joinNonEmpty(systemPrompt, section)
		}
	}

	return SubagentRun{
		Name:          e.config.Name,
		SystemPrompt:  systemPrompt,
		Task:          task,
		Tools:         finalTools,
		MaxTurns:      e.config.MaxTurns,
		ThreadID:      e.threadID,
		DeferredSetup: deferredSetup,
	}, nil
}

// Run 同步执行子任务(信号量限流,超出则阻塞等待)。对应简单委派路径。
func (e *SubagentExecutor) Run(ctx context.Context, task SubagentTask) (string, error) {
	select {
	case e.sem <- struct{}{}:
		defer func() { <-e.sem }()
	case <-ctx.Done():
		return "", ctx.Err()
	}
	run := SubagentRun{
		Name:         task.Type,
		SystemPrompt: e.config.SystemPrompt,
		Task:         task.Prompt,
		Tools:        e.baseTools,
		MaxTurns:     e.config.MaxTurns,
		ThreadID:     e.threadID,
	}
	outcome, err := e.runner(ctx, run, func([]capability.Message) {})
	return outcome.Result, err
}

// Execute 同步执行并返回结构化结果(对应 execute 的 asyncio.run 路径)。
func (e *SubagentExecutor) Execute(ctx context.Context, task string) *SubagentResult {
	result := NewSubagentResult(randomHex(8), e.traceID, SubagentRunning)
	result.SetRunning(time.Now())
	// 信号量限流(超出则阻塞等待),对应 MAX_CONCURRENT_SUBAGENTS。
	select {
	case e.sem <- struct{}{}:
		defer func() { <-e.sem }()
	case <-ctx.Done():
		result.TrySetTerminal(SubagentCancelled, "", "Cancelled by user", time.Now(), nil, nil)
		return result
	}
	// 同步路径不加超时(对应 execute 无事件循环时的 asyncio.run,无 future.result 超时);
	// 只尊重调用方 ctx 的取消。
	e.aexecute(ctx, task, result)
	return result
}

// ExecuteAsync 后台启动执行,返回 task_id(对应 execute_async)。
// taskID 为空时自动生成。
func (e *SubagentExecutor) ExecuteAsync(task string, taskID string) string {
	if taskID == "" {
		taskID = randomHex(8)
	}
	result := NewSubagentResult(taskID, e.traceID, SubagentPending)
	backgroundTasksMu.Lock()
	backgroundTasks[taskID] = result
	backgroundTasksMu.Unlock()

	go e.runBackground(taskID, task, result)
	return taskID
}

// runBackground 后台执行(对应 execute_async 提交到 scheduler pool 的 run_task):
// 置 RUNNING -> 信号量限流 -> 带超时执行。
func (e *SubagentExecutor) runBackground(taskID, task string, result *SubagentResult) {
	result.SetRunning(time.Now())

	timeout := time.Duration(e.config.TimeoutSeconds) * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	select {
	case e.sem <- struct{}{}:
		defer func() { <-e.sem }()
	case <-ctx.Done():
		result.TrySetTerminal(SubagentCancelled, "", "Cancelled by user", time.Now(), nil, nil)
		return
	}

	e.aexecute(ctx, task, result)
}

// aexecute 核心执行(对应 _aexecute):预检取消 -> 构建初始状态 -> 流式采集 AI 消息 -> 设终态。
func (e *SubagentExecutor) aexecute(ctx context.Context, task string, result *SubagentResult) {
	// 流式前预检:已取消则立即返回(对应 "cancelled before streaming")。
	if result.CancelRequested() {
		result.TrySetTerminal(SubagentCancelled, "", "Cancelled by user", time.Now(), nil, nil)
		return
	}

	run, err := e.buildInitialState(task)
	if err != nil {
		result.TrySetTerminal(SubagentFailed, "", err.Error(), time.Now(), nil, nil)
		return
	}

	var aiMessages []capability.Message
	outcome, runErr := e.runner(ctx, run, func(messages []capability.Message) {
		// 协作取消只在流边界被检测(对应 astream 迭代边界的取消检查)。
		if result.CancelRequested() {
			return
		}
		if len(messages) == 0 {
			return
		}
		last := messages[len(messages)-1]
		if last.Role != "assistant" {
			return
		}
		// 去重:Go 的 Message 无 id,退化为结构相等(对应按 id / model_dump 去重)。
		if !containsMessage(aiMessages, last) {
			aiMessages = append(aiMessages, last)
		}
	})

	if runErr != nil {
		switch {
		case errors.Is(runErr, context.DeadlineExceeded):
			result.TrySetTerminal(SubagentTimedOut, "", fmt.Sprintf("Execution timed out after %d seconds", e.config.TimeoutSeconds), time.Now(), aiMessages, nil)
		case errors.Is(runErr, context.Canceled) || result.CancelRequested():
			result.TrySetTerminal(SubagentCancelled, "", "Cancelled by user", time.Now(), aiMessages, nil)
		default:
			result.TrySetTerminal(SubagentFailed, "", runErr.Error(), time.Now(), aiMessages, nil)
		}
		return
	}

	if result.CancelRequested() {
		result.TrySetTerminal(SubagentCancelled, "", "Cancelled by user", time.Now(), aiMessages, nil)
		return
	}
	result.TrySetTerminal(SubagentCompleted, outcome.Result, "", time.Now(), aiMessages, outcome.TokenUsageRecords)
}

// containsMessage 线性判断消息是否已存在(对应 deer-flow 的 ai_messages 去重)。
func containsMessage(list []capability.Message, m capability.Message) bool {
	for _, x := range list {
		if messagesEqual(x, m) {
			return true
		}
	}
	return false
}

func messagesEqual(a, b capability.Message) bool {
	if a.Role != b.Role || a.Content != b.Content || len(a.ToolCalls) != len(b.ToolCalls) {
		return false
	}
	for i := range a.ToolCalls {
		if a.ToolCalls[i] != b.ToolCalls[i] {
			return false
		}
	}
	return true
}

// ── SSE 事件 ─────────────────────────────────────────────────────────────────

// SubagentEventType 是 SSE 事件类型(对应 task_tool 里 writer 的事件 type)。
type SubagentEventType string

const (
	SubagentEventStarted   SubagentEventType = "task_started"
	SubagentEventRunning   SubagentEventType = "task_running"
	SubagentEventCompleted SubagentEventType = "task_completed"
	SubagentEventFailed    SubagentEventType = "task_failed"
	SubagentEventCancelled SubagentEventType = "task_cancelled"
	SubagentEventTimedOut  SubagentEventType = "task_timed_out"
)

// SubagentEvent 是一次 SSE 事件(对应 writer({...}))。
type SubagentEvent struct {
	Type          SubagentEventType
	TaskID        string
	Description   string
	Message       capability.Message // task_running 携带
	MessageIndex  int                // 1-based
	TotalMessages int
	Result        string
	Error         string
	Usage         map[string]int
}

// SubagentEventSink 是 SSE 事件接收器。nil = 不发射。
type SubagentEventSink func(SubagentEvent)

// ── task 工具 ────────────────────────────────────────────────────────────────

// taskTool 是简单版 task 工具(对应 task_tool 的同步委派路径,演示/测试兼容)。
type taskTool struct {
	executor *SubagentExecutor
}

func (t taskTool) Name() string { return "task" }

func (t taskTool) Description() string {
	return "Delegate a task to a specialized subagent that runs in its own context. " +
		"Built-in types: general-purpose (complex multi-step tasks), bash (command execution). " +
		"Use for complex multi-step tasks that benefit from isolated context; do NOT use for simple single-step operations."
}

func (t taskTool) Run(ctx context.Context, argsJSON string) (string, error) {
	var args struct {
		Description  string `json:"description"`
		Prompt       string `json:"prompt"`
		SubagentType string `json:"subagent_type"`
	}
	if err := json.Unmarshal([]byte(argsJSON), &args); err != nil {
		return "", err
	}
	out, err := t.executor.Run(ctx, SubagentTask{
		Description: args.Description,
		Prompt:      args.Prompt,
		Type:        args.SubagentType,
	})
	if err != nil {
		return "", err
	}
	return out, nil
}

// NewTaskTool 把 SubagentExecutor 包装成 capability.Tool(同步委派)。
func NewTaskTool(executor *SubagentExecutor) capability.Tool {
	return taskTool{executor: executor}
}

// TaskToolOptions 是完整版 task 工具的构造参数(对应 task_tool 从 runtime 抽取的上下文)。
type TaskToolOptions struct {
	Tools           []capability.Tool
	Runner          SubagentRunner
	SubagentsCfg    *SubagentsAppConfig
	HostBashAllowed bool
	EventSink       SubagentEventSink // nil = 不发射 SSE 事件
	PollInterval    time.Duration     // 默认 5s
	MaxConcurrent   int               // 默认 3
}

// TaskTool 是完整版 task 工具(委派 + 后台执行 + 轮询 + SSE 事件)。
// 对应 task_tool.py::task_tool 的完整行为。
type TaskTool struct {
	opts TaskToolOptions
}

// NewTaskToolFull 构造完整版 task 工具。
func NewTaskToolFull(opts TaskToolOptions) *TaskTool {
	if opts.PollInterval <= 0 {
		opts.PollInterval = SubagentPollInterval
	}
	if opts.MaxConcurrent < 1 {
		opts.MaxConcurrent = MaxConcurrentSubagents
	}
	return &TaskTool{opts: opts}
}

func (t *TaskTool) Name() string { return "task" }

func (t *TaskTool) Description() string {
	return "Delegate a task to a specialized subagent that runs in its own context. " +
		"Built-in subagent types: general-purpose, bash. " +
		"Use for complex multi-step tasks that benefit from isolated context; do NOT use for simple single-step operations."
}

func (t *TaskTool) emit(ev SubagentEvent) {
	if t.opts.EventSink != nil {
		t.opts.EventSink(ev)
	}
}

// Run 委派任务并后台轮询到终态(对应 task_tool 的完整异步逻辑)。
func (t *TaskTool) Run(ctx context.Context, argsJSON string) (string, error) {
	var args struct {
		Description  string `json:"description"`
		Prompt       string `json:"prompt"`
		SubagentType string `json:"subagent_type"`
	}
	if err := json.Unmarshal([]byte(argsJSON), &args); err != nil {
		return "", err
	}

	availableNames := GetAvailableSubagentNames(t.opts.SubagentsCfg, t.opts.HostBashAllowed)
	config := GetSubagentConfig(args.SubagentType, t.opts.SubagentsCfg)
	if config == nil {
		return fmt.Sprintf("Error: Unknown subagent type '%s'. Available: %s", args.SubagentType, strings.Join(availableNames, ", ")), nil
	}
	if args.SubagentType == "bash" && !t.opts.HostBashAllowed {
		return "Error: " + LocalBashSubagentDisabledMessage, nil
	}

	executor := NewSubagentExecutorWithConfig(*config, t.opts.Tools, t.opts.Runner, t.opts.MaxConcurrent)

	// 后台启动(总是异步,避免阻塞主路径)。
	taskID := executor.ExecuteAsync(args.Prompt, "")

	t.emit(SubagentEvent{Type: SubagentEventStarted, TaskID: taskID, Description: args.Description})

	// 轮询超时:执行超时 + 60s 缓冲,按轮询间隔折算次数(对应 max_poll_count = (timeout+60)//5)。
	maxPolls := int(time.Duration(config.TimeoutSeconds+60) * time.Second / t.opts.PollInterval)
	if maxPolls < 1 {
		maxPolls = 1
	}
	lastMessageCount := 0

	for pollCount := 0; ; pollCount++ {
		if !sleepCtx(ctx, t.opts.PollInterval) {
			// 父任务被取消:请求协作取消后台子 agent 并返回。
			RequestCancelBackgroundTask(taskID)
			return "", ctx.Err()
		}

		result := GetBackgroundTaskResult(taskID)
		if result == nil {
			t.emit(SubagentEvent{Type: SubagentEventFailed, TaskID: taskID, Error: "Task disappeared from background tasks"})
			CleanupBackgroundTask(taskID)
			return fmt.Sprintf("Error: Task %s disappeared from background tasks", taskID), nil
		}

		// 新 AI 消息 -> task_running 事件。
		aiMessages := result.AIMessagesSnapshot()
		if len(aiMessages) > lastMessageCount {
			for i := lastMessageCount; i < len(aiMessages); i++ {
				t.emit(SubagentEvent{
					Type:          SubagentEventRunning,
					TaskID:        taskID,
					Message:       aiMessages[i],
					MessageIndex:  i + 1,
					TotalMessages: len(aiMessages),
				})
			}
			lastMessageCount = len(aiMessages)
		}

		usage, _ := SummarizeUsage(result.TokenUsageSnapshot())
		switch result.StatusValue() {
		case SubagentCompleted:
			t.emit(SubagentEvent{Type: SubagentEventCompleted, TaskID: taskID, Result: result.ResultValue(), Usage: usage})
			CleanupBackgroundTask(taskID)
			return "Task Succeeded. Result: " + result.ResultValue(), nil
		case SubagentFailed:
			t.emit(SubagentEvent{Type: SubagentEventFailed, TaskID: taskID, Error: result.ErrorValue(), Usage: usage})
			CleanupBackgroundTask(taskID)
			return "Task failed. Error: " + result.ErrorValue(), nil
		case SubagentCancelled:
			t.emit(SubagentEvent{Type: SubagentEventCancelled, TaskID: taskID, Error: result.ErrorValue(), Usage: usage})
			CleanupBackgroundTask(taskID)
			return "Task cancelled by user.", nil
		case SubagentTimedOut:
			t.emit(SubagentEvent{Type: SubagentEventTimedOut, TaskID: taskID, Error: result.ErrorValue(), Usage: usage})
			CleanupBackgroundTask(taskID)
			return "Task timed out. Error: " + result.ErrorValue(), nil
		}

		// 轮询超时安全网(线程池超时失效时兜底)。
		if pollCount > maxPolls {
			RequestCancelBackgroundTask(taskID)
			t.emit(SubagentEvent{Type: SubagentEventTimedOut, TaskID: taskID, Usage: usage})
			return fmt.Sprintf("Task polling timed out after %d minutes. This may indicate the background task is stuck.", config.TimeoutSeconds/60), nil
		}
	}
}

// sleepCtx 休眠指定时长,ctx 取消时返回 false。
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// ── 并发限流中间件(截断超额 task 调用)───────────────────────────────────────

// SubagentMinLimit / SubagentMaxLimit 对应 MIN_SUBAGENT_LIMIT / MAX_SUBAGENT_LIMIT。
const (
	SubagentMinLimit = 2
	SubagentMaxLimit = 4
)

// clampSubagentLimit 把并发上限 clamp 到 [2,4](对应 _clamp_subagent_limit)。
func clampSubagentLimit(value int) int {
	if value < SubagentMinLimit {
		return SubagentMinLimit
	}
	if value > SubagentMaxLimit {
		return SubagentMaxLimit
	}
	return value
}

// TruncateSubagentCalls 截断单个模型响应里超额的 task 调用(对应 SubagentLimitMiddleware._truncate_task_calls)。
// 保留前 maxConcurrent 个 task 调用,丢弃其余;maxConcurrent 被 clamp 到 [2,4]。
func TruncateSubagentCalls(toolCalls []capability.ToolCall, maxConcurrent int) []capability.ToolCall {
	limit := clampSubagentLimit(maxConcurrent)

	// 数出 task 调用的下标。
	var taskIndices []int
	for i, tc := range toolCalls {
		if tc.Name == "task" {
			taskIndices = append(taskIndices, i)
		}
	}
	if len(taskIndices) <= limit {
		return toolCalls
	}

	// 丢弃第 limit 个之后的 task 调用。
	drop := make(map[int]bool, len(taskIndices)-limit)
	for _, idx := range taskIndices[limit:] {
		drop[idx] = true
	}
	out := make([]capability.ToolCall, 0, len(toolCalls)-len(drop))
	for i, tc := range toolCalls {
		if !drop[i] {
			out = append(out, tc)
		}
	}
	return out
}

// ── 辅助 ─────────────────────────────────────────────────────────────────────
// containsString 由 loopdetect.go 提供(同包共用),此处不重复定义。

func joinNonEmpty(parts ...string) string {
	var out []string
	for _, p := range parts {
		if strings.TrimSpace(p) != "" {
			out = append(out, p)
		}
	}
	return strings.Join(out, "\n\n")
}
