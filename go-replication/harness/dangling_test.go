package harness

import (
	"strings"
	"testing"

	"deerflow-go/capability"
)

func TestPatchDanglingSynthesizesMissing(t *testing.T) {
	messages := []capability.Message{
		{Role: "user", Content: "hi"},
		{Role: "assistant", ToolCalls: []capability.ToolCall{{ID: "tc1", Name: "bash", Args: `{}`}}},
		// 缺 tc1 的 tool 结果(用户在工具执行时中断)。
	}
	patched := PatchDangling(messages)
	if len(patched) != 3 {
		t.Fatalf("expected 3 messages, got %d: %+v", len(patched), patched)
	}
	last := patched[len(patched)-1]
	if last.Role != "tool" || last.ToolCallID != "tc1" {
		t.Fatalf("expected synthesized tool message, got %+v", last)
	}
	if last.Content != interruptedMsg {
		t.Fatalf("unexpected content: %q", last.Content)
	}
}

func TestPatchDanglingNoChange(t *testing.T) {
	// 已良构(assistant.tool_calls 紧跟 tool 结果)→ 原样返回(同一底层切片)。
	messages := []capability.Message{
		{Role: "user", Content: "hi"},
		{Role: "assistant", ToolCalls: []capability.ToolCall{{ID: "tc1", Name: "bash", Args: `{}`}}},
		{Role: "tool", ToolCallID: "tc1", Name: "bash", Content: "ok"},
	}
	patched := PatchDangling(messages)
	if len(patched) != len(messages) {
		t.Fatalf("expected no change, got %d messages", len(patched))
	}
	for i := range patched {
		if patched[i].Role != messages[i].Role || patched[i].Content != messages[i].Content {
			t.Fatalf("message %d changed", i)
		}
	}
}

func TestPatchDanglingCausalReorder(t *testing.T) {
	// 两个 assistant 都带 tool_calls,tool 结果堆在末尾 → 重放成因果顺序。
	messages := []capability.Message{
		{Role: "user", Content: "hi"},
		{Role: "assistant", ToolCalls: []capability.ToolCall{{ID: "tc1", Name: "bash", Args: `{}`}}},
		{Role: "assistant", ToolCalls: []capability.ToolCall{{ID: "tc2", Name: "bash", Args: `{}`}}},
		{Role: "tool", ToolCallID: "tc1", Name: "bash", Content: "r1"},
		{Role: "tool", ToolCallID: "tc2", Name: "bash", Content: "r2"},
	}
	patched := PatchDangling(messages)
	if len(patched) != 5 {
		t.Fatalf("expected 5, got %d", len(patched))
	}
	// 期望顺序:user, A(tc1), tool(tc1), B(tc2), tool(tc2)。
	want := []struct{ role, id, content string }{
		{"user", "", "hi"},
		{"assistant", "", ""},
		{"tool", "tc1", "r1"},
		{"assistant", "", ""},
		{"tool", "tc2", "r2"},
	}
	for i, w := range want {
		if patched[i].Role != w.role {
			t.Fatalf("index %d: role %q want %q", i, patched[i].Role, w.role)
		}
		if w.id != "" && patched[i].ToolCallID != w.id {
			t.Fatalf("index %d: id %q want %q", i, patched[i].ToolCallID, w.id)
		}
		if w.content != "" && patched[i].Content != w.content {
			t.Fatalf("index %d: content %q want %q", i, patched[i].Content, w.content)
		}
	}
}

func TestSyntheticWriteFileTruncation(t *testing.T) {
	longErr := strings.Repeat("x", 1000)
	msg := DanglingMessage{
		Role: "assistant",
		InvalidToolCalls: []InvalidToolCall{
			{ID: "bad", Name: "write_file", Error: longErr},
		},
	}
	patched := PatchDanglingMessages([]DanglingMessage{msg})
	if patched == nil || len(patched) != 2 {
		t.Fatalf("expected 2 messages, got %+v", patched)
	}
	content := patched[1].Content
	if !strings.HasPrefix(content, writeFileInvalidBase) {
		t.Fatalf("expected write_file base, got prefix: %.80q", content)
	}
	// 错误详情截断到 500 字符,不把 1000 字符原样回填。
	if strings.Contains(content, strings.Repeat("x", 501)) {
		t.Fatal("error detail should be truncated to 500 runes")
	}
	if !strings.Contains(content, " Parser error: "+strings.Repeat("x", 500)) {
		t.Fatal("expected 500-char parser error detail")
	}
}

func TestSyntheticInvalidGeneric(t *testing.T) {
	msg := DanglingMessage{
		Role: "assistant",
		InvalidToolCalls: []InvalidToolCall{
			{ID: "bad", Name: "some_tool", Error: "boom"},
		},
	}
	patched := PatchDanglingMessages([]DanglingMessage{msg})
	if patched == nil || len(patched) != 2 {
		t.Fatalf("expected 2 messages, got %+v", patched)
	}
	if patched[1].Content != "[Tool call could not be executed because its arguments were invalid: boom]" {
		t.Fatalf("unexpected content: %q", patched[1].Content)
	}
}

func TestMessageToolCallsRawFallback(t *testing.T) {
	fn := &RawFunction{Name: "bash"}
	msg := DanglingMessage{
		Role:         "assistant",
		ToolCalls:    nil, // 结构化为空 → 回退 raw
		RawToolCalls: []RawToolCall{{ID: "rc1", Name: "", Function: fn}},
	}
	calls := messageToolCalls(msg)
	if len(calls) != 1 || calls[0].ID != "rc1" || calls[0].Name != "bash" {
		t.Fatalf("raw fallback: %+v", calls)
	}
	// 有结构化 tool_calls 时 raw 被忽略。
	msg2 := DanglingMessage{
		Role:         "assistant",
		ToolCalls:    []capability.ToolCall{{ID: "s1", Name: "structured"}},
		RawToolCalls: []RawToolCall{{ID: "rc1", Function: fn}},
	}
	calls2 := messageToolCalls(msg2)
	if len(calls2) != 1 || calls2[0].ID != "s1" {
		t.Fatalf("structured should win: %+v", calls2)
	}
}
