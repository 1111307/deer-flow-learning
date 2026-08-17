package harness

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"deerflow-go/capability"
)

// scriptedModel 按脚本顺序返回响应。
type scriptedModel struct {
	responses []capability.ChatResponse
	idx       int
	requests  []capability.ChatRequest
}

func (m *scriptedModel) Name() string { return "scripted" }
func (m *scriptedModel) Chat(_ context.Context, req capability.ChatRequest) (*capability.ChatResponse, error) {
	m.requests = append(m.requests, req)
	if m.idx >= len(m.responses) {
		return &capability.ChatResponse{Message: capability.Message{Role: "assistant", Content: "final"}}, nil
	}
	r := m.responses[m.idx]
	m.idx++
	return &r, nil
}

// echoTool 记录调用并返回固定输出。
type echoTool struct{ calls int }

func (e *echoTool) Name() string        { return "echo" }
func (e *echoTool) Description() string { return "echo tool" }
func (e *echoTool) Run(_ context.Context, argsJSON string) (string, error) {
	e.calls++
	return "echo-output", nil
}

func TestRunSimpleTurn(t *testing.T) {
	m := &scriptedModel{responses: []capability.ChatResponse{
		{Message: capability.Message{Role: "assistant", Content: "hello there"}},
	}}
	a := New(m, map[string]capability.Tool{}, nil)

	thread := &Thread{ID: "t1"}
	res, err := a.Run(context.Background(), thread, "hi")
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if res.Reply != "hello there" {
		t.Fatalf("expected reply, got %q", res.Reply)
	}
	if res.Interrupt != nil {
		t.Fatalf("unexpected interrupt: %+v", res.Interrupt)
	}
}

func TestRunToolCallTurn(t *testing.T) {
	m := &scriptedModel{responses: []capability.ChatResponse{
		{Message: capability.Message{Role: "assistant", ToolCalls: []capability.ToolCall{{ID: "c1", Name: "echo", Args: `{}`}}}},
		{Message: capability.Message{Role: "assistant", Content: "done after tool"}},
	}}
	et := &echoTool{}
	a := New(m, map[string]capability.Tool{"echo": et}, nil)

	thread := &Thread{ID: "t1"}
	res, err := a.Run(context.Background(), thread, "run tool")
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if et.calls != 1 {
		t.Fatalf("expected 1 tool call, got %d", et.calls)
	}
	if res.Reply != "done after tool" {
		t.Fatalf("expected reply, got %q", res.Reply)
	}
	// tool 消息应紧跟 assistant.tool_calls 配对(悬空检查)。
	var lastToolCallID string
	for _, msg := range thread.Messages {
		if len(msg.ToolCalls) > 0 {
			lastToolCallID = msg.ToolCalls[0].ID
		}
		if msg.Role == "tool" && msg.ToolCallID != lastToolCallID {
			t.Fatalf("tool message %q not paired with assistant tool_call", msg.ToolCallID)
		}
	}
}

func TestRunInterruptAndResume(t *testing.T) {
	// 第一轮:模型调用 ask_clarification → 挂起。第二轮(resume):模型返回文本。
	m := &scriptedModel{responses: []capability.ChatResponse{
		{Message: capability.Message{Role: "assistant", ToolCalls: []capability.ToolCall{{ID: "clar", Name: "ask_clarification", Args: `{"question":"confirm?","clarification_type":"risk_confirmation"}`}}}},
		{Message: capability.Message{Role: "assistant", Content: "proceeding"}},
	}}
	a := New(m, map[string]capability.Tool{"ask_clarification": ClarificationTool()}, nil)
	a.Middleware = []ToolMiddleware{ClarificationMiddleware()}

	thread := &Thread{ID: "t1"}
	res, err := a.Run(context.Background(), thread, "delete production")
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if res.Interrupt == nil {
		t.Fatal("expected interrupt")
	}
	if res.Interrupt.Type != capability.ClarifyRiskConfirmation {
		t.Fatalf("expected risk_confirmation, got %q", res.Interrupt.Type)
	}

	// resume 后继续,拿到文本回复。
	res2, err := a.Resume(context.Background(), thread, "yes, confirmed")
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if res2.Reply != "proceeding" {
		t.Fatalf("expected reply after resume, got %q", res2.Reply)
	}
}

func TestRunRecursionLimit(t *testing.T) {
	// 模型永远返回工具调用 → 触发 recursion_limit。
	m := &scriptedModel{}
	for i := 0; i < 200; i++ {
		m.responses = append(m.responses, capability.ChatResponse{
			Message: capability.Message{Role: "assistant", ToolCalls: []capability.ToolCall{{ID: "c", Name: "echo", Args: `{}`}}},
		})
	}
	a := New(m, map[string]capability.Tool{"echo": &echoTool{}}, nil)
	a.MaxSteps = 5 // 小上限便于测试。

	thread := &Thread{ID: "t1"}
	_, err := a.Run(context.Background(), thread, "loop")
	if err == nil || !strings.Contains(err.Error(), "recursion_limit") {
		t.Fatalf("expected recursion_limit error, got %v", err)
	}
}

func TestRunUnknownTool(t *testing.T) {
	m := &scriptedModel{responses: []capability.ChatResponse{
		{Message: capability.Message{Role: "assistant", ToolCalls: []capability.ToolCall{{ID: "u1", Name: "nope", Args: `{}`}}}},
		{Message: capability.Message{Role: "assistant", Content: "recovered"}},
	}}
	a := New(m, map[string]capability.Tool{}, nil)

	thread := &Thread{ID: "t1"}
	res, err := a.Run(context.Background(), thread, "go")
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if res.Reply != "recovered" {
		t.Fatalf("expected recovered reply, got %q", res.Reply)
	}
	// 未知工具应产生一条可读错误 tool 消息让模型纠偏,而不是崩 run。
	var sawUnknown bool
	for _, msg := range thread.Messages {
		if msg.Role == "tool" && strings.Contains(msg.Content, "unknown tool") {
			sawUnknown = true
		}
	}
	if !sawUnknown {
		t.Fatal("expected unknown-tool error message")
	}
}

// 确保 chain 顺序正确(外层→内层)。
func TestChainOrder(t *testing.T) {
	var order []string
	mw := func(name string) ToolMiddleware {
		return func(next ToolHandler) ToolHandler {
			return func(ctx context.Context, call capability.ToolCall) (string, *capability.InterruptRequest, error) {
				order = append(order, name+"-before")
				out, intr, err := next(ctx, call)
				order = append(order, name+"-after")
				return out, intr, err
			}
		}
	}
	base := func(ctx context.Context, call capability.ToolCall) (string, *capability.InterruptRequest, error) {
		order = append(order, "base")
		return "ok", nil, nil
	}
	h := chain(base, []ToolMiddleware{mw("outer"), mw("inner")})
	_, _, _ = h(context.Background(), capability.ToolCall{Name: "x"})

	want := []string{"outer-before", "inner-before", "base", "inner-after", "outer-after"}
	gotJSON, _ := json.Marshal(order)
	wantJSON, _ := json.Marshal(want)
	if string(gotJSON) != string(wantJSON) {
		t.Fatalf("unexpected order: got %s, want %s", gotJSON, wantJSON)
	}
}
