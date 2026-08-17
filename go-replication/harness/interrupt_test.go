package harness

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"deerflow-go/capability"
)

func TestFormatClarificationMessage(t *testing.T) {
	args := clarificationArgs{
		Question:          "which approach?",
		ClarificationType: capability.ClarifyApproachChoice,
		Options:           []string{"option A", "option B"},
	}
	got := formatClarificationMessage(args)
	if !strings.Contains(got, "which approach?") {
		t.Fatalf("expected question, got %q", got)
	}
	if !strings.Contains(got, "1. option A") || !strings.Contains(got, "2. option B") {
		t.Fatalf("expected options list, got %q", got)
	}
	if !strings.Contains(got, "🔀") {
		t.Fatalf("expected approach_choice icon, got %q", got)
	}

	// 有 context:context 前置。
	withCtx := clarificationArgs{Question: "q?", Context: "background", ClarificationType: capability.ClarifyRiskConfirmation}
	got = formatClarificationMessage(withCtx)
	if !strings.Contains(got, "⚠️ background") {
		t.Fatalf("expected risk icon + context, got %q", got)
	}
}

func TestNormalizeOptions(t *testing.T) {
	// JSON 字符串数组 → []string(对应 Qwen3-Max 把数组序列化成字符串)。
	if got := normalizeOptions(`["a","b"]`); len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("unexpected JSON-string normalization: %v", got)
	}
	// 原生列表。
	if got := normalizeOptions([]string{"x", "y"}); len(got) != 2 {
		t.Fatalf("unexpected []string normalization: %v", got)
	}
	// nil → 空。
	if got := normalizeOptions(nil); got != nil {
		t.Fatalf("expected nil for nil options, got %v", got)
	}
	// 非 JSON 字符串 → 单元素。
	if got := normalizeOptions("just one"); len(got) != 1 || got[0] != "just one" {
		t.Fatalf("unexpected non-JSON string normalization: %v", got)
	}
}

func TestStableMessageID(t *testing.T) {
	a := stableMessageID("call-1", "formatted")
	b := stableMessageID("call-1", "different")
	if a != b {
		t.Fatalf("same tool_call_id should yield same id: %q vs %q", a, b)
	}
	// 无 tool_call_id → 基于消息内容哈希。
	c := stableMessageID("", "msg-x")
	if !strings.HasPrefix(c, "clarification:") {
		t.Fatalf("expected clarification: prefix, got %q", c)
	}
}

func TestClarificationMiddlewareIntercept(t *testing.T) {
	var executed bool
	base := func(ctx context.Context, call capability.ToolCall) (string, *capability.InterruptRequest, error) {
		executed = true
		return "executed", nil, nil
	}
	mw := ClarificationMiddleware()
	handler := mw(base)

	// 非 ask_clarification → 放行执行。
	_, _, err := handler(context.Background(), capability.ToolCall{Name: "bash", Args: `{}`})
	if err != nil || !executed {
		t.Fatalf("expected passthrough, executed=%v err=%v", executed, err)
	}

	// ask_clarification → 拦截返回 interrupt,不执行工具。
	executed = false
	args, _ := json.Marshal(map[string]any{
		"question":           "confirm delete?",
		"clarification_type": "risk_confirmation",
		"options":            []string{"yes", "no"},
	})
	out, interrupt, err := handler(context.Background(), capability.ToolCall{Name: "ask_clarification", ID: "tc1", Args: string(args)})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if executed {
		t.Fatal("ask_clarification should not execute the base tool")
	}
	if out != "" || interrupt == nil {
		t.Fatalf("expected interrupt, got out=%q interrupt=%+v", out, interrupt)
	}
	if interrupt.Type != capability.ClarifyRiskConfirmation {
		t.Fatalf("expected risk_confirmation, got %q", interrupt.Type)
	}
	if !strings.Contains(interrupt.Question, "confirm delete?") {
		t.Fatalf("expected formatted question, got %q", interrupt.Question)
	}
}
