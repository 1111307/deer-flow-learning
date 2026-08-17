// human-in-the-loop(人工决策挂起 + 审批链)。
//
// 对应 deer-flow:
//   - tools/builtins/clarification_tool.py::ask_clarification_tool(占位 + return_direct)
//   - agents/middlewares/clarification_middleware.py::ClarificationMiddleware
//     用 Command(goto=END) 中断 graph,把问题抛给前端
//
// 机制:模型不直接「挂起」,而是像调用普通工具一样调用 ask_clarification。
// ClarificationMiddleware 拦截这个调用,不真正执行工具,而是返回一个
// InterruptRequest;loop 检测到 interrupt 就把 run 挂起,等用户响应后 Resume。
//
// 「审批链」:risk_confirmation 类型对应「危险操作需人工确认」——
// 模型在删除文件、修改生产前先 ask_clarification(risk_confirmation),
// 拿到用户明确同意才继续。这就是把「危险动作」纳入人工审批链。
//
// 关键生产细节(和 clarification_middleware.py 逐行对应):
//   - _format_clarification_message:类型图标 + context 前置 + options 格式化。
//   - options 可能是 JSON 字符串(某些模型如 Qwen3-Max 把数组序列化成字符串),
//     需要反序列化归一化成 []string。
//   - _stable_message_id:确定性消息 id,重试的澄清调用「替换」而非「追加」。
//   - 拦截逻辑:非 ask_clarification 放行给 next,ask_clarification 返回 interrupt。
package harness

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"deerflow-go/capability"
)

// clarificationTool 是 ask_clarification 工具的占位实现。
// 对应 deer-flow clarification_tool.py:return_direct=True,真正的逻辑在中间件里。
type clarificationTool struct{}

func (clarificationTool) Name() string { return "ask_clarification" }
func (clarificationTool) Description() string {
	return "请求人工澄清或确认(危险操作必须确认)。调用后 run 挂起,等待用户回答。"
}
func (clarificationTool) Run(_ context.Context, _ string) (string, error) {
	// 占位:正常情况下不会被真正执行,因为 ClarificationMiddleware 会先拦截。
	return "Clarification request processed by middleware", nil
}

// ClarificationTool 返回 ask_clarification 工具实例,供 Agent 注册到 Tools map。
func ClarificationTool() capability.Tool { return clarificationTool{} }

// clarificationArgs 是 ask_clarification 的参数结构。
// 对应 clarification_tool.py 的参数:question / clarification_type / context / options。
type clarificationArgs struct {
	Question          string      `json:"question"`
	ClarificationType string      `json:"clarification_type"`
	Context           string      `json:"context"`
	Options           interface{} `json:"options"` // 可能是 []string,也可能是 JSON 字符串
}

// stableMessageID 生成确定性消息 id(对应 _stable_message_id):
// 有 tool_call_id 用 "clarification:{id}",否则用格式化消息的 sha256 前 16 位。
// 语义:重试的澄清调用用同一 id,「替换」而非「追加」重复的 ToolMessage。
func stableMessageID(toolCallID, formattedMessage string) string {
	if toolCallID != "" {
		return "clarification:" + toolCallID
	}
	sum := sha256.Sum256([]byte(formattedMessage))
	return "clarification:" + hex.EncodeToString(sum[:])[:16]
}

// isChinese 检测文本是否含中文字符(对应 _is_chinese)。
func isChinese(text string) bool {
	for _, r := range text {
		if r >= '\u4e00' && r <= '\u9fff' {
			return true
		}
	}
	return false
}

// normalizeOptions 把 options 归一化成 []string(对应 _format_clarification_message
// 里的 options 反序列化逻辑):
//   - JSON 字符串 → 反序列化(失败则退化为单元素 [原始串])
//   - nil → 空列表
//   - 非列表 → 包成单元素
func normalizeOptions(raw interface{}) []string {
	switch v := raw.(type) {
	case nil:
		return nil
	case []string:
		return v
	case []interface{}:
		out := make([]string, 0, len(v))
		for _, item := range v {
			if s, ok := item.(string); ok {
				out = append(out, s)
			} else {
				out = append(out, fmt.Sprintf("%v", item))
			}
		}
		return out
	case string:
		var list []string
		if err := json.Unmarshal([]byte(v), &list); err == nil {
			return list
		}
		// 非 JSON 字符串:退化为单元素。
		if v == "" {
			return nil
		}
		return []string{v}
	default:
		return []string{fmt.Sprintf("%v", raw)}
	}
}

// typeIcons 澄清类型的图标(对应 type_icons)。
var typeIcons = map[string]string{
	capability.ClarifyMissingInfo:          "❓",
	capability.ClarifyAmbiguousRequirement: "🤔",
	capability.ClarifyApproachChoice:       "🔀",
	capability.ClarifyRiskConfirmation:     "⚠️",
	capability.ClarifySuggestion:           "💡",
}

// formatClarificationMessage 把澄清参数格式化成用户友好消息(对应
// _format_clarification_message):图标 + context 前置 + question + options 列表。
func formatClarificationMessage(args clarificationArgs) string {
	question := args.Question
	clarificationType := args.ClarificationType
	if clarificationType == "" {
		clarificationType = capability.ClarifyMissingInfo
	}
	options := normalizeOptions(args.Options)

	icon := typeIcons[clarificationType]
	if icon == "" {
		icon = "❓"
	}

	var parts []string
	if args.Context != "" {
		// 有 context:先给背景,再给问题。
		parts = append(parts, icon+" "+args.Context)
		parts = append(parts, "\n"+question)
	} else {
		parts = append(parts, icon+" "+question)
	}

	if len(options) > 0 {
		parts = append(parts, "") // 空行
		for i, opt := range options {
			parts = append(parts, fmt.Sprintf("  %d. %s", i+1, opt))
		}
	}
	return strings.Join(parts, "\n")
}

// ClarificationMiddleware 拦截 ask_clarification,把工具调用转成 interrupt。
// 对应 deer-flow clarification_middleware.py::ClarificationMiddleware.wrap_tool_call:
// 非 ask_clarification 放行给 next,ask_clarification 则返回 interrupt。
//
// 它必须排在最内层(chain 的最后一个),因为它是「殿后」拦截器 —— 对应 deer-flow
// 注释:ClarificationMiddleware should always be last。
func ClarificationMiddleware() ToolMiddleware {
	return func(next ToolHandler) ToolHandler {
		return func(ctx context.Context, call capability.ToolCall) (string, *capability.InterruptRequest, error) {
			if call.Name != "ask_clarification" {
				return next(ctx, call)
			}

			var args clarificationArgs
			_ = json.Unmarshal([]byte(call.Args), &args)
			if args.ClarificationType == "" {
				args.ClarificationType = capability.ClarifyMissingInfo
			}

			formatted := formatClarificationMessage(args)

			// 生成确定性消息 id(对应 _stable_message_id)——用于替换而非追加。
			_ = stableMessageID(call.ID, formatted)

			// 返回 interrupt:Question 携带格式化消息,loop 会用其回填 tool 消息
			// (对应 deer-flow Command(update={messages:[tool_message]}, goto=END))。
			return "", &capability.InterruptRequest{
				Type:     args.ClarificationType,
				Question: formatted,
				Options:  normalizeOptions(args.Options),
			}, nil
		}
	}
}
