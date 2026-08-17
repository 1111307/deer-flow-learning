// 护栏 Guardrail —— 对应 deer-flow guardrails/middleware.py + builtin.py + provider.py。
//
// 源码行号映射:
//   - GuardrailRequest / GuardrailReason / GuardrailDecision / GuardrailProvider
//     : provider.py:9-56(Protocol + dataclass)
//   - AllowlistProvider  : builtin.py:6-24(allowed/denied 名单,无外部依赖)
//   - GuardrailMiddleware : middleware.py:20-98(wrap_tool_call 执行前 evaluate + fail_closed)
//
// 核心设计:
//
//	在工具「真正执行前」对调用做鉴权(GuardrailProvider.evaluate),deny 时返回一条
//	error 消息让 agent 改道,而不是直接崩掉 run。provider 抛异常时的行为由 fail_closed 决定:
//	  - true(默认):阻断(安全优先)—— 返回一条 oap.evaluator_error 的 deny 消息。
//	  - false:放行(可用性优先)—— 记警告后执行工具。
//
//	deny 的语义是「给模型一条可读的错误 ToolMessage,让模型换一种方式重试」,
//	而不是失败整个 run —— 这是安全系统的经典权衡。
//
// Go 与 Python 的关键差异:
//   - Python 的 wrap_tool_call 返回 ToolMessage | Command;Go 里 ToolHandler 返回
//     out string(错误消息内容),harness 据此回填 tool 结果。
//   - Python 的 async aevaluate(Protocol)在 Go 里没有 async/await,收敛为单个
//     Evaluate(ctx 由 goroutine 语义承载);ToolHandler 本身是同步的。
//   - Python 的 GraphBubbleUp(LangGraph 控制流信号,interrupt/pause/resume)在 Go 里
//     表达为 GraphBubbleUp 错误类型;中间件用 errors.As 识别后透传,绝不落入
//     fail_closed 逻辑。
//   - ToolMessage 的 status="error" / name 字段在 Go 的 Message 模型里没有;
//     name 由 harness 回填工具名,status 的「错误」语义由错误内容文本承载。
package harness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"deerflow-go/capability"
)

// GuardrailRequest 一次鉴权请求(对应 provider.py::GuardrailRequest)。
//   - ToolInput 对应 Python 的 dict[str, Any](工具参数)。
//   - ThreadID / IsSubagent 由中间件保持零值 —— 对应 Python 里 _build_request 未设置,
//     使用 dataclass 默认 None/False。
type GuardrailRequest struct {
	ToolName   string
	ToolInput  map[string]any
	AgentID    string // 对应 _build_request 的 agent_id=self.passport
	ThreadID   string
	IsSubagent bool
	Timestamp  string
}

// GuardrailReason 是 allow/deny 决策的结构化原因(对应 provider.py::GuardrailReason,
// 对齐 OAP reason 对象)。
type GuardrailReason struct {
	Code    string
	Message string
}

// GuardrailDecision 是 provider 的 allow/deny 结论(对应 provider.py::GuardrailDecision)。
type GuardrailDecision struct {
	Allow    bool
	Reasons  []GuardrailReason
	PolicyID string
	Metadata map[string]any
}

// GuardrailProvider 是「可插拔工具鉴权」的契约(对应 provider.py::GuardrailProvider Protocol)。
// Evaluate 返回 (结论, error);error 非 nil 时由中间件按 fail_closed 分支处理。
type GuardrailProvider interface {
	Name() string
	Evaluate(req GuardrailRequest) (GuardrailDecision, error)
}

// GraphBubbleUp 对应 LangGraph 的 GraphBubbleUp 控制流信号(interrupt/pause/resume)。
// provider 在 evaluate 里「抛」出它(Go 里作为 error 返回),中间件必须透传,
// 不能被 fail_closed 逻辑当作普通异常吞掉。
type GraphBubbleUp struct {
	Interrupt *capability.InterruptRequest
}

func (g *GraphBubbleUp) Error() string {
	return "graph bubble up: LangGraph control-flow signal"
}

// AllowlistProvider 是允许/拒绝名单提供者(对应 builtin.py::AllowlistProvider)。
//   - allowed 为 nil(或空)= 不启用允许名单(不限);
//   - denied 恒为集合(空集合 = 无黑名单)。
type AllowlistProvider struct {
	allowed map[string]struct{} // nil = 不启用允许名单(不限)
	denied  map[string]struct{}
}

// NewAllowlistProvider 构造名单提供者。对应 builtin.py::__init__。
// allowed 传 nil 或空 slice 表示「不限」;denied 传 nil 或空 slice 表示「无黑名单」。
// (Python 用 `if allowed_tools` 判空:None 与 [] 都视为「不启用」。)
func NewAllowlistProvider(allowed, denied []string) *AllowlistProvider {
	p := &AllowlistProvider{denied: map[string]struct{}{}}
	if len(allowed) > 0 {
		p.allowed = map[string]struct{}{}
		for _, n := range allowed {
			p.allowed[n] = struct{}{}
		}
	}
	for _, n := range denied {
		p.denied[n] = struct{}{}
	}
	return p
}

// Name 返回 provider 名(对应 builtin.py 的 name = "allowlist")。
func (p *AllowlistProvider) Name() string { return "allowlist" }

// Evaluate 执行允许/拒绝判断(对应 builtin.py::evaluate)。
// 允许名单优先于拒绝名单:不在允许名单 → deny;在拒绝名单 → deny;否则 allow。
func (p *AllowlistProvider) Evaluate(req GuardrailRequest) (GuardrailDecision, error) {
	if p.allowed != nil {
		if _, ok := p.allowed[req.ToolName]; !ok {
			return GuardrailDecision{
				Allow:   false,
				Reasons: []GuardrailReason{{Code: "oap.tool_not_allowed", Message: fmt.Sprintf("tool '%s' not in allowlist", req.ToolName)}},
			}, nil
		}
	}
	if _, ok := p.denied[req.ToolName]; ok {
		return GuardrailDecision{
			Allow:   false,
			Reasons: []GuardrailReason{{Code: "oap.tool_not_allowed", Message: fmt.Sprintf("tool '%s' is denied", req.ToolName)}},
		}, nil
	}
	return GuardrailDecision{
		Allow:   true,
		Reasons: []GuardrailReason{{Code: "oap.allowed"}},
	}, nil
}

// GuardrailMiddleware 在工具执行前鉴权(对应 middleware.py::GuardrailMiddleware)。
//
// deny 返回一条 error 消息(不执行工具),让 agent 换一种方式重试。
// provider 抛异常时:
//   - failClosed=true(默认):阻断,返回 oap.evaluator_error 的 deny 消息。
//   - failClosed=false:放行(可用性优先)。
//
// GraphBubbleUp 透传(见 GraphBubbleUp)。
//
// passport 是可选参数(对应 __init__ 的 passport=None),作为请求的 agent_id。
func GuardrailMiddleware(provider GuardrailProvider, failClosed bool, passport ...string) ToolMiddleware {
	p := ""
	if len(passport) > 0 {
		p = passport[0]
	}
	return func(next ToolHandler) ToolHandler {
		return func(ctx context.Context, call capability.ToolCall) (string, *capability.InterruptRequest, error) {
			gr := buildGuardrailRequest(call, p)

			decision, err := provider.Evaluate(gr)
			if err != nil {
				var bubble *GraphBubbleUp
				if errors.As(err, &bubble) {
					// 透传 LangGraph 控制流信号,绝不落入 fail_closed。
					if bubble.Interrupt != nil {
						return "", bubble.Interrupt, nil
					}
					return "", nil, bubble
				}
				// provider 异常:fail_closed 决定阻断还是放行。
				if failClosed {
					decision = GuardrailDecision{
						Allow:   false,
						Reasons: []GuardrailReason{{Code: "oap.evaluator_error", Message: "guardrail provider error (fail-closed)"}},
					}
				} else {
					return next(ctx, call) // fail-open:放行
				}
			}

			if !decision.Allow {
				return buildDeniedMessage(call, decision), nil, nil
			}
			return next(ctx, call)
		}
	}
}

// buildGuardrailRequest 复现 middleware.py::_build_request。
// 只设置 tool_name / tool_input / agent_id / timestamp;thread_id / is_subagent 保持零值。
func buildGuardrailRequest(call capability.ToolCall, passport string) GuardrailRequest {
	var input map[string]any
	if call.Args != "" {
		_ = json.Unmarshal([]byte(call.Args), &input)
	}
	if input == nil {
		input = map[string]any{}
	}
	return GuardrailRequest{
		ToolName:  call.Name,
		ToolInput: input,
		AgentID:   passport,
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
	}
}

// buildDeniedMessage 复现 middleware.py::_build_denied_message,返回错误消息内容。
// 工具名缺省为 "unknown_tool";reason 缺省为 "blocked by guardrail policy" /
// 代码 "oap.denied"。
func buildDeniedMessage(call capability.ToolCall, decision GuardrailDecision) string {
	toolName := call.Name
	if toolName == "" {
		toolName = "unknown_tool"
	}
	reasonText := "blocked by guardrail policy"
	reasonCode := "oap.denied"
	if len(decision.Reasons) > 0 {
		reasonText = decision.Reasons[0].Message
		reasonCode = decision.Reasons[0].Code
	}
	return fmt.Sprintf(
		"Guardrail denied: tool '%s' was blocked (%s). Reason: %s. Choose an alternative approach.",
		toolName, reasonCode, reasonText,
	)
}
