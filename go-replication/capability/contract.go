// Package capability 是「capability-seam」的能力三件套之一:Service Definition(服务定义)。
//
// 对应 deer-flow:
//   - sandbox/sandbox.py::Sandbox(ABC)             —— 沙盒能力的抽象契约(execute_command/
//     read_file/download_file/list_dir/write_file/glob/grep/update_file 8 个方法)
//   - sandbox/sandbox_provider.py::SandboxProvider(ABC) —— 沙盒生命周期契约(acquire/get/release/reset)
//   - LangChain 的 BaseChatModel / BaseTool          —— 模型与工具的抽象契约
//
// Go 里没有 ABC,interface 就是天然的「服务定义」:只声明「能做什么」,
// 不关心「怎么做」。capability-seam 的本质是依赖倒置 —— 上层(harness/consumer)
// 只依赖这些接口,具体实现由 provider 层注入。
//
// 本文件是**所有子 agent 的共享契约**:Message / ToolCall / Tool / ModelProvider /
// ChatRequest / ChatResponse / InterruptRequest 的字段名与方法签名被 harness 的
// loopdetect / dangling / memory / guardrail / subagent 等文件依赖,改动必须同步所有使用方。
// Sandbox / SandboxProvider 沙盒契约由 provider/sandbox.go 消费。
package capability

import "context"

// GrepMatch 一次 grep 命中(对应 sandbox/search.py::GrepMatch)。
type GrepMatch struct {
	Path       string
	LineNumber int
	Line       string
}

// Sandbox 是沙盒能力的服务定义。对应 sandbox.py::Sandbox,
// 本地进程实现(LocalSandbox)与容器实现(AioSandbox)都遵守同一份契约。
//
// 关键差异(与 Python 的语义对齐):
//   - ExecuteCommand 返回 string 而不是 (string, error):deer-flow 里 execute_command
//     捕获一切异常,把 stderr / Exit Code / "(no output)" 拼进输出返回给模型,
//     而不是让 run 直接崩掉 —— 模型看到失败输出后自行判断下一步。
//   - ReadFile / WriteFile / DownloadFile / Glob / Grep / UpdateFile 返回 error,
//     对应 Python 里 raise OSError / PermissionError(如路径穿越、只读文件系统)。
//   - ListDir 返回 []string 而非 ([]string, error):deer-flow 里目录不存在时返回空列表。
type Sandbox interface {
	// ID 返回沙盒唯一标识。
	ID() string
	// ExecuteCommand 在沙盒里执行一条命令,返回 stdout/stderr(含退出码/空输出标记)。
	ExecuteCommand(command string) string
	// ReadFile 读取文件内容(UTF-8)。路径穿越/不存在时返回 error。
	ReadFile(path string) (string, error)
	// DownloadFile 下载二进制内容。路径穿越/越界时返回 PermissionError 语义的 error。
	DownloadFile(path string) ([]byte, error)
	// ListDir 列出目录内容(最多 maxDepth 层,含目录尾 "/" 标记)。目录不存在返回空。
	ListDir(path string, maxDepth int) []string
	// WriteFile 写文件(content 全量覆盖或 append)。
	WriteFile(path string, content string, appendMode bool) error
	// Glob 在 root 下按 glob 模式匹配路径,返回 (matches, truncated)。
	Glob(path string, pattern string, includeDirs bool, maxResults int) ([]string, bool, error)
	// Grep 在目录内搜索匹配行,返回 (matches, truncated)。
	Grep(path string, pattern string, glob string, literal bool, caseSensitive bool, maxResults int) ([]GrepMatch, bool, error)
	// UpdateFile 以二进制内容更新文件(对应 update_file)。
	UpdateFile(path string, content []byte) error
}

// SandboxProvider 是沙盒的生命周期管理契约(acquire/get/release/reset)。
// 对应 sandbox_provider.py::SandboxProvider。
type SandboxProvider interface {
	// Acquire 获取(或创建)一个沙盒,返回沙盒 id。
	// deer-flow 里 per-thread 复用:同一个 thread 多次 acquire 拿到同一个沙盒。
	Acquire(threadID string) (string, error)
	// Get 按 id 取回沙盒实例。
	Get(id string) (Sandbox, bool)
	// Release 释放沙盒(本地直接丢弃,容器可能进 warm pool 复用)。
	Release(id string)
	// Reset 清空跨 provider 实例存活的状态(如 LocalSandboxProvider 缓存的沙盒)。
	// 对应 reset_sandbox_provider() 里调用的 provider.reset()。
	Reset()
}

// SandboxShutdowner 是可选的优雅关闭接口。shutdown_sandbox_provider() 通过
// 类型断言检查 provider 是否实现了它(对应 Python 的 hasattr(provider, "shutdown"))。
type SandboxShutdowner interface {
	Shutdown()
}

// ModelProvider 是「AI 供应商」角色的服务定义。
// 对应 deer-flow 的 models/factory.py::create_chat_model 背后统一的
// BaseChatModel —— harness 只依赖这个接口,不关心底层是 OpenAI / DeepSeek /
// Anthropic 还是本地 vLLM。
type ModelProvider interface {
	// Name 返回供应商/模型标识(用于日志与摘要选择)。
	Name() string
	// Chat 发起一次补全。messages 是完整消息历史(含工具调用/结果)。
	Chat(ctx context.Context, req ChatRequest) (*ChatResponse, error)
}

// ChatRequest 一次模型调用的输入。
type ChatRequest struct {
	Messages []Message
	// Model 允许 harness 指定模型名(空 = 供应商默认)。对应 deer-flow 的 model_name。
	Model string
}

// Message 一条消息。Role 取值:system/user/assistant/tool。
type Message struct {
	Role    string
	Content string
	// ToolCalls 由 assistant 消息携带(模型决定调用哪些工具)。
	ToolCalls []ToolCall
	// ToolCallID 由 tool 消息携带(工具执行结果回填给哪个调用)。
	ToolCallID string
	// Name 工具名(仅 tool 消息有意义)。
	Name string
}

// ToolCall 一次工具调用请求(对应 LLM function calling 的一个 tool_call)。
type ToolCall struct {
	ID   string
	Name string
	Args string // JSON 编码的参数
}

// ChatResponse 一次模型调用的输出。
type ChatResponse struct {
	Message Message
	// Interrupt 非 nil 表示模型请求人工介入(对应 deer-flow 的 ask_clarification)。
	// harness 看到它就把整个 run 挂起,等待用户响应后再 resume。
	Interrupt *InterruptRequest
}

// InterruptRequest 人工介入请求(对应 clarification_tool.py 的 ask_clarification 参数)。
type InterruptRequest struct {
	// Type 对应 deer-flow 的 clarification_type:
	// missing_info / ambiguous_requirement / approach_choice / risk_confirmation / suggestion。
	Type     string
	Question string
	Options  []string
}

// 澄清类型常量(对应 clarification_tool.py 的 Literal 枚举)。
const (
	ClarifyMissingInfo          = "missing_info"
	ClarifyAmbiguousRequirement = "ambiguous_requirement"
	ClarifyApproachChoice       = "approach_choice"
	ClarifyRiskConfirmation     = "risk_confirmation"
	ClarifySuggestion           = "suggestion"
)

// Tool 是「能力消费者」视角的工具契约。
// 对应 deer-flow tools/builtins 下的 bash_tool / read_file / write_file / task_tool。
type Tool interface {
	Name() string
	Description() string
	// Run 执行工具,argsJSON 为模型给出的参数(JSON)。
	Run(ctx context.Context, argsJSON string) (string, error)
}

// threadIDKey 是 context 中携带当前 thread_id 的 key(沙盒工具解析沙盒时使用)。
//
// 对应 deer-flow 里 ensure_sandbox_initialized(runtime) 从 runtime.context["thread_id"]
// 读取 thread_id。Go 的 Tool.Run 只拿到 ctx,所以 thread 上下文必须走 ctx 传递。
// harness 在调用工具前注入(WithThreadID),provider 里的沙盒工具读取(ThreadIDFrom)。
type threadIDKey struct{}

// WithThreadID 把当前 thread_id 注入 ctx,供沙盒工具按线程复用沙盒。
func WithThreadID(ctx context.Context, threadID string) context.Context {
	if threadID == "" {
		return ctx
	}
	return context.WithValue(ctx, threadIDKey{}, threadID)
}

// ThreadIDFrom 从 ctx 取回 thread_id(无则返回空串)。
func ThreadIDFrom(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	id, _ := ctx.Value(threadIDKey{}).(string)
	return id
}
