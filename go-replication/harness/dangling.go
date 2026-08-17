// Dangling Tool Call 补偿 —— 对应 deer-flow agents/middlewares/dangling_tool_call_middleware.py
// (整文件)。
//
// 源码行号映射:
//   - _MAX_RECOVERY_ERROR_DETAIL_LEN : dangling_tool_call_middleware.py:32
//   - _message_tool_calls           : :43-101(含 additional_kwargs raw payload 与 invalid_tool_calls)
//   - _synthetic_tool_message_content : :103-126(write_file 大 payload 截断 500 字符 / invalid 错误提示)
//   - _build_patched_messages       : :128-183(按因果顺序重放,缺失补合成 error ToolMessage)
//   - wrap_model_call               : :185-205
//
// 核心设计:
//
//	用户在 agent 调用工具过程中中断请求,消息历史出现「AIMessage 带 tool_calls 但没有
//	对应 ToolMessage」的悬空调用,所有主流 provider(OpenAI/Moonshot/Anthropic)会 400 拒绝。
//	解法:发下一次模型请求前(wrap_model_call)扫描历史,为悬空调用补一条合成的
//	error ToolMessage,让协议形式合法,同时让模型知道「这次调用被打断/参数非法」。
//
//	关键(和 deer-flow 一致):必须在 wrap_model_call 阶段按「因果顺序」重放 tool 消息
//	(AI 消息 → 紧跟其 tool 结果),而不是 before_model 阶段简单 append 到末尾 ——
//	append 到末尾同样会破坏「assistant.tool_calls 紧跟 tool 结果」的配对约束。
//
// Go 与 Python 的关键差异:
//   - Python 的 msg.type("ai"/"tool"/"human") 对应 Go 的 Role("assistant"/"tool"/"user")。
//   - Python 的 additional_kwargs["tool_calls"] 与 invalid_tool_calls 是 LangChain 消息的
//     独立字段;Go 的 capability.Message 没有,所以本文件扩展出 DanglingMessage /
//     RawToolCall / InvalidToolCall 承载。公开的 PatchDangling 只消费 capability.Message
//     (主路径:结构化 tool_calls 悬空);raw/invalid 路径由 PatchDanglingMessages 复现。
//   - Python 的 _message_tool_calls 会解析 raw function.arguments 的 args 字典,但
//     dangling 逻辑只读 id/name/invalid/error,从不读 args,故 Go 里省略 args 解析。
package harness

import (
	"fmt"
	"reflect"

	"deerflow-go/capability"
)

// maxRecoveryErrorDetailLen 对应 dangling_tool_call_middleware.py:32。
// 畸形 write_file 调用可能携带巨大 Markdown payload,合成的错误详情必须截断,
// 不要把大段/畸形内容原样回填给模型(issue #2894 的防御)。
const maxRecoveryErrorDetailLen = 500

// 合成错误消息模板 —— 对应 _synthetic_tool_message_content 的逐字字符串。
const (
	// write_fileInvalidBase 是 write_file 参数非法时的完整提示(不含末尾 details 与 "]").
	writeFileInvalidBase = "[write_file failed before execution: the tool-call arguments were not valid JSON, so no file was written. This often happens when the model tries to write a very large Markdown file in a single tool call, especially when `content` contains unescaped quotes, inline JSON, backslashes, or code fences. Do not retry the same large `write_file` payload for this artifact; provide the report/content directly as normal assistant text in your next response. If a file write is still needed later, split the file into smaller sections instead of one large payload."

	invalidArgsNoError   = "[Tool call could not be executed because its arguments were invalid.]"
	invalidArgsWithError = "[Tool call could not be executed because its arguments were invalid: %s]"
	interruptedMsg       = "[Tool call was interrupted and did not return a result.]"
)

// RawFunction 对应 raw tool call 的 function 子对象(OpenAI additional_kwargs.tool_calls[i].function)。
// Arguments 是 JSON 字符串;dangling 逻辑只读 Name,不读 Arguments,故此处仅保留结构。
type RawFunction struct {
	Name      string
	Arguments string
}

// RawToolCall 对应 additional_kwargs["tool_calls"] 的单个 raw 元素。
// Function 用 *RawFunction:nil = 无 function 字段(对应 raw_tc.get("function") 为 None)。
type RawToolCall struct {
	ID       string
	Name     string
	Function *RawFunction
}

// InvalidToolCall 对应 LangChain 消息的 invalid_tool_calls 元素(畸形工具调用)。
type InvalidToolCall struct {
	ID    string
	Name  string
	Error string
}

// DanglingMessage 是 dangling 补偿中间件操作的增强消息,扩展 capability.Message:
//   - RawToolCalls 对应 additional_kwargs["tool_calls"](仅结构化 tool_calls 为空时回退)。
//   - InvalidToolCalls 对应 invalid_tool_calls(畸形调用,视为悬空,需补 error ToolMessage)。
type DanglingMessage struct {
	Role       string
	Content    string
	ToolCalls  []capability.ToolCall
	ToolCallID string
	Name       string

	RawToolCalls     []RawToolCall
	InvalidToolCalls []InvalidToolCall
}

// danglingCall 是 _message_tool_calls 归一化后的单次工具调用。
// Python 的 dict 还携带 args 字段,但 dangling 逻辑从不读 args,故这里只保留
// id / name / invalid / error。
type danglingCall struct {
	ID      string
	Name    string
	Invalid bool
	Error   string
}

// PatchDangling 扫描消息历史,为悬空 tool_call 补合成错误消息(公开接口)。
// 返回补丁后的消息;若无需修补,原样返回(不分配新 slice)。
func PatchDangling(messages []capability.Message) []capability.Message {
	dms := make([]DanglingMessage, len(messages))
	for i, m := range messages {
		dms[i] = DanglingMessage{Role: m.Role, Content: m.Content, ToolCalls: m.ToolCalls, ToolCallID: m.ToolCallID, Name: m.Name}
	}
	patched := patchDangling(dms)
	if patched == nil {
		return messages
	}
	out := make([]capability.Message, len(patched))
	for i, dm := range patched {
		out[i] = capability.Message{Role: dm.Role, Content: dm.Content, ToolCalls: dm.ToolCalls, ToolCallID: dm.ToolCallID, Name: dm.Name}
	}
	return out
}

// PatchDanglingMessages 是 PatchDangling 的增强版,支持 raw / invalid tool calls。
// 用于复现 _message_tool_calls 的完整语义;返回 nil 表示无需修补。
func PatchDanglingMessages(messages []DanglingMessage) []DanglingMessage {
	return patchDangling(messages)
}

// patchDangling 复现 _build_patched_messages:按因果顺序重放 tool 消息,缺失补合成。
// 返回 nil 表示无需修补(patched == messages)。
func patchDangling(messages []DanglingMessage) []DanglingMessage {
	// 1. 按 tool_call_id 分组已有 tool 消息(队列,对应 defaultdict(deque))。
	toolMessagesByID := map[string][]DanglingMessage{}
	for _, m := range messages {
		if m.Role == "tool" {
			toolMessagesByID[m.ToolCallID] = append(toolMessagesByID[m.ToolCallID], m)
		}
	}

	// 2. 收集所有 assistant 消息的 tool_call id(含 raw / invalid)。
	toolCallIDs := map[string]bool{}
	for _, m := range messages {
		if m.Role != "assistant" {
			continue
		}
		for _, tc := range messageToolCalls(m) {
			if tc.ID != "" {
				toolCallIDs[tc.ID] = true
			}
		}
	}

	// 3. 因果顺序重放:跳过独立的 tool 消息(它们会在下面按「AI 消息 → 紧跟 tool 结果」
	//    重放),每个 assistant 消息之后紧跟其 tool 结果;缺失的补合成 error 消息。
	patched := make([]DanglingMessage, 0, len(messages)+4)
	for _, m := range messages {
		if m.Role == "tool" && toolCallIDs[m.ToolCallID] {
			continue
		}
		patched = append(patched, m)
		if m.Role != "assistant" {
			continue
		}
		for _, tc := range messageToolCalls(m) {
			if tc.ID == "" {
				continue
			}
			queue := toolMessagesByID[tc.ID]
			if len(queue) > 0 {
				patched = append(patched, queue[0])
				toolMessagesByID[tc.ID] = queue[1:]
			} else {
				name := tc.Name
				if name == "" {
					name = "unknown"
				}
				patched = append(patched, DanglingMessage{
					Role:       "tool",
					ToolCallID: tc.ID,
					Name:       name,
					Content:    syntheticToolMessageContent(tc),
				})
			}
		}
	}

	if reflect.DeepEqual(patched, messages) {
		return nil
	}
	return patched
}

// messageToolCalls 复现 _message_tool_calls:从结构化字段 / raw provider payload /
// invalid_tool_calls 归一化出工具调用列表。
func messageToolCalls(dm DanglingMessage) []danglingCall {
	var out []danglingCall

	// 1. 结构化 tool_calls(对应 msg.tool_calls)。
	for _, tc := range dm.ToolCalls {
		out = append(out, danglingCall{ID: tc.ID, Name: tc.Name})
	}

	// 2. raw additional_kwargs.tool_calls(仅当结构化 tool_calls 为空时回退)。
	if len(dm.ToolCalls) == 0 {
		for _, raw := range dm.RawToolCalls {
			name := raw.Name
			if name == "" && raw.Function != nil {
				name = raw.Function.Name
			}
			if name == "" {
				name = "unknown"
			}
			out = append(out, danglingCall{ID: raw.ID, Name: name})
		}
	}

	// 3. invalid_tool_calls(畸形调用,视为悬空,需补可恢复错误)。
	for _, inv := range dm.InvalidToolCalls {
		name := inv.Name
		if name == "" {
			name = "unknown"
		}
		out = append(out, danglingCall{ID: inv.ID, Name: name, Invalid: true, Error: inv.Error})
	}

	return out
}

// syntheticToolMessageContent 复现 _synthetic_tool_message_content。
//   - invalid 且 write_file:超大 payload 特化的恢复指引,错误详情截断 500 字符。
//   - invalid 且其它工具:参数非法错误(带/不带详情)。
//   - 非 invalid:调用被打断。
func syntheticToolMessageContent(tc danglingCall) string {
	if tc.Invalid {
		errorText := ""
		if tc.Error != "" {
			errorText = truncateRunes(tc.Error, maxRecoveryErrorDetailLen)
		}
		if tc.Name == "write_file" {
			details := ""
			if errorText != "" {
				details = " Parser error: " + errorText
			}
			return writeFileInvalidBase + details + "]"
		}
		if errorText != "" {
			return fmt.Sprintf(invalidArgsWithError, errorText)
		}
		return invalidArgsNoError
	}
	return interruptedMsg
}

// truncateRunes 按「字符」(rune)截断,避免在 UTF-8 多字节字符中间切断。
// 对应 Python 的 error[:_MAX_RECOVERY_ERROR_DETAIL_LEN](按 code point 截断)。
func truncateRunes(s string, maxRunes int) string {
	r := []rune(s)
	if len(r) <= maxRunes {
		return s
	}
	return string(r[:maxRunes])
}
