// 中间件责任链 —— 对应 deer-flow 的 19 级 middleware 链。
//
// deer-flow 用 AgentMiddleware.wrap_tool_call 把横切关注点(循环检测、安全、
// 澄清拦截、摘要……)按 append 顺序叠在「工具执行」这个核心动作外面。
// Go 的惯用形态是函数式中间件:一个 func(next ToolHandler) ToolHandler。
//
// 与 deer-flow 的对应关系:
//   - ToolHandler      ≈ handler(request) -> ToolMessage | Command
//   - ToolMiddleware   ≈ AgentMiddleware.wrap_tool_call
//   - chain            ≈ 把 mws 列表按「外层→内层」顺序串成责任链
//
// 关键差异:Python 的 middleware 是「继承 AgentMiddleware + override wrap_tool_call」,
// Go 用「高阶函数」表达同一件事 —— 更轻、无继承,且天然可组合/可测试。
package harness

import (
	"context"

	"deerflow-go/capability"
)

// ToolHandler 是「执行一次工具调用」的处理器签名。
// 返回:
//   - out:工具输出(给模型看的文本)
//   - interrupt:非 nil 表示该调用请求人工介入(挂起整个 run)
type ToolHandler func(ctx context.Context, call capability.ToolCall) (out string, interrupt *capability.InterruptRequest, err error)

// ToolMiddleware 包装一个 ToolHandler,返回新的 ToolHandler。
// 对应 deer-flow 的 AgentMiddleware.wrap_tool_call。
type ToolMiddleware func(next ToolHandler) ToolHandler

// chain 把中间件按顺序串成责任链。
// 约定:mws[0] 最外层(最先拦截)。对应 deer-flow 里 append 顺序越靠前越先执行。
//
// deer-flow 有一个关键约束:ClarificationMiddleware 必须排最后(最内层),
// 因为它是「殿后」的拦截器 —— 任何前面中间件放行的工具调用最终都会经过它。
// 这里的 chain 用「从后往前包」实现:mws 列表顺序 = 外层→内层。
func chain(handler ToolHandler, mws []ToolMiddleware) ToolHandler {
	for i := len(mws) - 1; i >= 0; i-- {
		handler = mws[i](handler)
	}
	return handler
}

// AuditMiddleware 示例:给每次工具执行打印一条审计日志(对应 SandboxAuditMiddleware)。
// 它展示「不改变语义,只做横切」的中间件怎么写。
func AuditMiddleware() ToolMiddleware {
	return func(next ToolHandler) ToolHandler {
		return func(ctx context.Context, call capability.ToolCall) (string, *capability.InterruptRequest, error) {
			out, interrupt, err := next(ctx, call)
			// 真实实现会落审计日志(谁、何时、调了什么工具、结果摘要)。
			_ = out
			return out, interrupt, err
		}
	}
}
