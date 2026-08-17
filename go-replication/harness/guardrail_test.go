package harness

import (
	"context"
	"strings"
	"testing"

	"deerflow-go/capability"
)

func baseHandler() ToolHandler {
	return func(_ context.Context, call capability.ToolCall) (string, *capability.InterruptRequest, error) {
		return "executed " + call.Name, nil, nil
	}
}

func TestAllowlistProvider(t *testing.T) {
	p := NewAllowlistProvider([]string{"read_file", "bash"}, []string{"bash"})

	// 在允许名单且不在拒绝名单 → allow。
	d, err := p.Evaluate(GuardrailRequest{ToolName: "read_file"})
	if err != nil || !d.Allow {
		t.Fatalf("read_file should be allowed: %+v %v", d, err)
	}
	if d.Reasons[0].Code != "oap.allowed" {
		t.Fatalf("code: %s", d.Reasons[0].Code)
	}
	// 不在允许名单 → deny。
	d, _ = p.Evaluate(GuardrailRequest{ToolName: "delete_db"})
	if d.Allow {
		t.Fatal("delete_db should be denied (not in allowlist)")
	}
	if d.Reasons[0].Message != "tool 'delete_db' not in allowlist" {
		t.Fatalf("message: %s", d.Reasons[0].Message)
	}
	// 在允许名单但在拒绝名单 → deny(拒绝名单优先)。
	d, _ = p.Evaluate(GuardrailRequest{ToolName: "bash"})
	if d.Allow {
		t.Fatal("bash should be denied (in denylist)")
	}
	if d.Reasons[0].Message != "tool 'bash' is denied" {
		t.Fatalf("message: %s", d.Reasons[0].Message)
	}
}

func TestAllowlistProviderNoAllowlist(t *testing.T) {
	// allowed 为 nil(或空)→ 不限。
	p := NewAllowlistProvider(nil, []string{"bash"})
	if d, _ := p.Evaluate(GuardrailRequest{ToolName: "read_file"}); !d.Allow {
		t.Fatal("read_file should be allowed (no allowlist)")
	}
	if d, _ := p.Evaluate(GuardrailRequest{ToolName: "bash"}); d.Allow {
		t.Fatal("bash should be denied")
	}
}

func TestGuardrailMiddlewareDenyAndAllow(t *testing.T) {
	allowlist := NewAllowlistProvider([]string{"bash"}, nil)
	mw := GuardrailMiddleware(allowlist, true)
	handler := mw(baseHandler())

	// 允许 → 执行工具。
	out, interrupt, err := handler(context.Background(), capability.ToolCall{Name: "bash", Args: "{}"})
	if err != nil || interrupt != nil || out != "executed bash" {
		t.Fatalf("allowed: out=%q interrupt=%v err=%v", out, interrupt, err)
	}
	// 拒绝 → 返回错误消息,不执行工具。
	out, interrupt, err = handler(context.Background(), capability.ToolCall{Name: "delete_db", Args: "{}"})
	if err != nil || interrupt != nil {
		t.Fatalf("denied should not error: out=%q err=%v", out, err)
	}
	if !strings.Contains(out, "Guardrail denied: tool 'delete_db' was blocked (oap.tool_not_allowed)") {
		t.Fatalf("denied message: %q", out)
	}
}

// failingProvider 始终返回 error(触发 fail_closed / fail_open 分支)。
type failingProvider struct{}

func (failingProvider) Name() string { return "failing" }
func (failingProvider) Evaluate(GuardrailRequest) (GuardrailDecision, error) {
	return GuardrailDecision{}, context.Canceled // 任意 error
}

func TestGuardrailFailClosed(t *testing.T) {
	handler := GuardrailMiddleware(failingProvider{}, true)(baseHandler())
	out, _, _ := handler(context.Background(), capability.ToolCall{Name: "bash", Args: "{}"})
	if !strings.Contains(out, "oap.evaluator_error") {
		t.Fatalf("fail-closed should deny with evaluator_error: %q", out)
	}
}

func TestGuardrailFailOpen(t *testing.T) {
	handler := GuardrailMiddleware(failingProvider{}, false)(baseHandler())
	out, _, err := handler(context.Background(), capability.ToolCall{Name: "bash", Args: "{}"})
	if err != nil || out != "executed bash" {
		t.Fatalf("fail-open should execute: out=%q err=%v", out, err)
	}
}

func TestGuardrailGraphBubbleUpPassthrough(t *testing.T) {
	// provider 抛出 GraphBubbleUp(带 interrupt)→ 必须透传,不能落入 fail_closed。
	bubbleProvider := bubbleProvider{interrupt: &capability.InterruptRequest{Type: "risk_confirmation", Question: "confirm?"}}
	handler := GuardrailMiddleware(bubbleProvider, true)(baseHandler())
	out, interrupt, err := handler(context.Background(), capability.ToolCall{Name: "bash", Args: "{}"})
	if err != nil {
		t.Fatalf("bubble-up should not be error: %v", err)
	}
	if interrupt == nil || interrupt.Question != "confirm?" {
		t.Fatalf("interrupt should be passed through: out=%q interrupt=%v", out, interrupt)
	}
}

// bubbleProvider 返回 GraphBubbleUp 错误(模拟 LangGraph 控制流信号)。
type bubbleProvider struct {
	interrupt *capability.InterruptRequest
}

func (b bubbleProvider) Name() string { return "bubble" }
func (b bubbleProvider) Evaluate(GuardrailRequest) (GuardrailDecision, error) {
	return GuardrailDecision{}, &GraphBubbleUp{Interrupt: b.interrupt}
}
