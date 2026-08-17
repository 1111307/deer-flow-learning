package harness

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"deerflow-go/capability"
)

// ── 配置与注册表 ────────────────────────────────────────────────────────────

func TestGetSubagentConfigBuiltins(t *testing.T) {
	gp := GetSubagentConfig("general-purpose", nil)
	if gp == nil {
		t.Fatalf("general-purpose should exist")
	}
	if gp.MaxTurns != 150 {
		t.Fatalf("general-purpose max_turns should be 150, got %d", gp.MaxTurns)
	}
	bash := GetSubagentConfig("bash", nil)
	if bash == nil || bash.MaxTurns != 60 {
		t.Fatalf("bash config wrong: %+v", bash)
	}
	if GetSubagentConfig("unknown", nil) != nil {
		t.Fatalf("unknown subagent should return nil")
	}
}

func TestGetSubagentConfigOverrides(t *testing.T) {
	cfg := &SubagentsAppConfig{
		TimeoutSeconds: 1800,
		Agents: map[string]SubagentOverride{
			"general-purpose": {MaxTurns: intPtr(200)},
		},
	}
	gp := GetSubagentConfig("general-purpose", cfg)
	// 内置超时:全局默认 1800 覆盖配置自带的 900。
	if gp.TimeoutSeconds != 1800 {
		t.Fatalf("builtin timeout should be global 1800, got %d", gp.TimeoutSeconds)
	}
	// per-agent max_turns 覆盖。
	if gp.MaxTurns != 200 {
		t.Fatalf("per-agent max_turns override should be 200, got %d", gp.MaxTurns)
	}
}

func TestGetSubagentConfigCustom(t *testing.T) {
	cfg := &SubagentsAppConfig{
		CustomAgents: map[string]SubagentConfig{
			"reviewer": {Name: "reviewer", Model: "gpt-4", TimeoutSeconds: 600, MaxTurns: 20},
		},
		TimeoutSeconds: 1800,
	}
	rev := GetSubagentConfig("reviewer", cfg)
	if rev == nil {
		t.Fatalf("custom agent should resolve")
	}
	// 自定义 agent 不用全局默认覆盖,保留自身值。
	if rev.TimeoutSeconds != 600 {
		t.Fatalf("custom agent timeout should stay 600, got %d", rev.TimeoutSeconds)
	}
}

func TestGetAvailableSubagentNamesBashGating(t *testing.T) {
	names := GetAvailableSubagentNames(nil, false)
	for _, n := range names {
		if n == "bash" {
			t.Fatalf("bash should be hidden when host bash not allowed")
		}
	}
	allowed := GetAvailableSubagentNames(nil, true)
	if !containsString(allowed, "bash") {
		t.Fatalf("bash should be visible when host bash allowed")
	}
}

func TestResolveSubagentModelName(t *testing.T) {
	if got := ResolveSubagentModelName(SubagentConfig{Model: "gpt-4"}, "parent"); got != "gpt-4" {
		t.Fatalf("explicit model wins: %q", got)
	}
	if got := ResolveSubagentModelName(SubagentConfig{Model: "inherit"}, "parent"); got != "parent" {
		t.Fatalf("inherit should use parent: %q", got)
	}
	if got := ResolveSubagentModelName(SubagentConfig{Model: "inherit"}, ""); got != "" {
		t.Fatalf("no default model: %q", got)
	}
}

// ── 并发限流截断 ────────────────────────────────────────────────────────────

func TestTruncateSubagentCalls(t *testing.T) {
	calls := []capability.ToolCall{
		{Name: "task", ID: "t1"},
		{Name: "read_file", ID: "r1"},
		{Name: "task", ID: "t2"},
		{Name: "task", ID: "t3"},
		{Name: "task", ID: "t4"}, // 第 4 个 task,应被截断
		{Name: "task", ID: "t5"}, // 第 5 个 task,应被截断
	}
	got := TruncateSubagentCalls(calls, 3)
	taskCount := 0
	for _, c := range got {
		if c.Name == "task" {
			taskCount++
		}
	}
	if taskCount != 3 {
		t.Fatalf("should keep 3 task calls, got %d: %+v", taskCount, got)
	}
	// 非 task 调用保留。
	if !containsToolCallID(got, "r1") {
		t.Fatalf("non-task call should be preserved")
	}
	if containsToolCallID(got, "t4") || containsToolCallID(got, "t5") {
		t.Fatalf("excess task calls should be truncated")
	}
}

func TestTruncateSubagentCallsClamp(t *testing.T) {
	// clamp 到 [2,4]:传入 100 → 4。
	calls := []capability.ToolCall{}
	for i := 0; i < 6; i++ {
		calls = append(calls, capability.ToolCall{Name: "task", ID: "x"})
	}
	if got := TruncateSubagentCalls(calls, 100); len(got) != 4 {
		t.Fatalf("limit should clamp to 4, got %d", len(got))
	}
	if got := TruncateSubagentCalls(calls, 1); len(got) != 2 {
		t.Fatalf("limit should clamp to 2, got %d", len(got))
	}
}

func containsToolCallID(calls []capability.ToolCall, id string) bool {
	for _, c := range calls {
		if c.ID == id {
			return true
		}
	}
	return false
}

// ── 执行器 ───────────────────────────────────────────────────────────────────

func TestSubagentExecutorRunSync(t *testing.T) {
	exec := NewSubagentExecutor(3, func(_ context.Context, task SubagentTask) (string, error) {
		return "done: " + task.Prompt, nil
	})
	out, err := exec.Run(context.Background(), SubagentTask{Description: "d", Prompt: "p", Type: "general-purpose"})
	if err != nil || out != "done: p" {
		t.Fatalf("sync run: out=%q err=%v", out, err)
	}
}

func TestSubagentResultTrySetTerminalFirstWins(t *testing.T) {
	r := NewSubagentResult("id", "trace", SubagentRunning)
	if !r.TrySetTerminal(SubagentCompleted, "first", "", time.Now(), nil, nil) {
		t.Fatalf("first terminal should succeed")
	}
	if r.TrySetTerminal(SubagentFailed, "", "late error", time.Now(), nil, nil) {
		t.Fatalf("second terminal should fail (first wins)")
	}
	if r.StatusValue() != SubagentCompleted || r.ResultValue() != "first" {
		t.Fatalf("late write should not change status: %s %q", r.StatusValue(), r.ResultValue())
	}
}

func TestSubagentResultNonTerminalRejected(t *testing.T) {
	r := NewSubagentResult("id", "trace", SubagentRunning)
	if r.TrySetTerminal(SubagentRunning, "x", "", time.Now(), nil, nil) {
		t.Fatalf("non-terminal status should be rejected")
	}
}

func TestSubagentExecuteStreamsAIMessages(t *testing.T) {
	runner := func(_ context.Context, run SubagentRun, emit func([]capability.Message)) (SubagentRunResult, error) {
		if emit != nil {
			emit([]capability.Message{{Role: "assistant", Content: "step 1"}})
			emit([]capability.Message{{Role: "assistant", Content: "step 1"}, {Role: "assistant", Content: "step 2"}})
		}
		return SubagentRunResult{Result: "final"}, nil
	}
	exec := NewSubagentExecutorWithConfig(GeneralPurposeConfig, nil, runner, 3)

	result := exec.Execute(context.Background(), "do it")
	if result.StatusValue() != SubagentCompleted {
		t.Fatalf("expected completed, got %s", result.StatusValue())
	}
	if result.ResultValue() != "final" {
		t.Fatalf("result wrong: %q", result.ResultValue())
	}
	msgs := result.AIMessagesSnapshot()
	if len(msgs) != 2 {
		t.Fatalf("expected 2 deduped AI messages, got %d", len(msgs))
	}
}

func TestSubagentExecuteFailure(t *testing.T) {
	runner := func(context.Context, SubagentRun, func([]capability.Message)) (SubagentRunResult, error) {
		return SubagentRunResult{}, errors.New("boom")
	}
	exec := NewSubagentExecutorWithConfig(GeneralPurposeConfig, nil, runner, 3)
	result := exec.Execute(context.Background(), "do it")
	if result.StatusValue() != SubagentFailed || result.ErrorValue() != "boom" {
		t.Fatalf("expected failed, got %s %q", result.StatusValue(), result.ErrorValue())
	}
}

func TestSubagentExecuteTimeout(t *testing.T) {
	// runner 阻塞等待 ctx,超时后应置 TIMED_OUT。
	runner := func(ctx context.Context, _ SubagentRun, _ func([]capability.Message)) (SubagentRunResult, error) {
		<-ctx.Done()
		return SubagentRunResult{}, ctx.Err()
	}
	cfg := GeneralPurposeConfig
	cfg.TimeoutSeconds = 1
	exec := NewSubagentExecutorWithConfig(cfg, nil, runner, 3)

	// 走后台路径(带超时):ExecuteAsync 里用 context.WithTimeout。
	taskID := exec.ExecuteAsync("do it", "")
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		r := GetBackgroundTaskResult(taskID)
		if r != nil && r.IsTerminalValue() {
			if r.StatusValue() != SubagentTimedOut {
				t.Fatalf("expected timed_out, got %s", r.StatusValue())
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for background task timeout")
}

func TestSubagentConcurrencyLimit(t *testing.T) {
	const limit = 3
	var active, maxActive int32
	var mu sync.Mutex

	runner := func(ctx context.Context, _ SubagentRun, _ func([]capability.Message)) (SubagentRunResult, error) {
		mu.Lock()
		active++
		if active > maxActive {
			maxActive = active
		}
		mu.Unlock()
		time.Sleep(30 * time.Millisecond)
		mu.Lock()
		active--
		mu.Unlock()
		return SubagentRunResult{Result: "ok"}, nil
	}
	exec := NewSubagentExecutorWithConfig(GeneralPurposeConfig, nil, runner, limit)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			exec.Execute(context.Background(), "task")
		}()
	}
	wg.Wait()
	if maxActive > limit {
		t.Fatalf("concurrency exceeded limit: maxActive=%d limit=%d", maxActive, limit)
	}
}

// ── task 工具(完整版:委派 + 轮询 + SSE 事件)────────────────────────────────

func TestTaskToolFullDelegationAndEvents(t *testing.T) {
	var events []SubagentEvent
	runner := func(_ context.Context, run SubagentRun, emit func([]capability.Message)) (SubagentRunResult, error) {
		if emit != nil {
			emit([]capability.Message{{Role: "assistant", Content: "working"}})
		}
		return SubagentRunResult{
			Result:            "all done",
			TokenUsageRecords: []TokenUsageRecord{{InputTokens: 10, OutputTokens: 5, TotalTokens: 15}},
		}, nil
	}
	tool := NewTaskToolFull(TaskToolOptions{
		Runner:          runner,
		HostBashAllowed: true,
		PollInterval:    10 * time.Millisecond,
		EventSink:       func(ev SubagentEvent) { events = append(events, ev) },
	})

	out, err := tool.Run(context.Background(), `{"description":"d","prompt":"do the thing","subagent_type":"general-purpose"}`)
	if err != nil {
		t.Fatalf("task tool error: %v", err)
	}
	if !strings.Contains(out, "all done") {
		t.Fatalf("task tool output wrong: %q", out)
	}

	// 事件序列:started -> running -> completed。
	if len(events) < 3 {
		t.Fatalf("expected at least 3 events, got %d", len(events))
	}
	if events[0].Type != SubagentEventStarted {
		t.Fatalf("first event should be started, got %s", events[0].Type)
	}
	if events[len(events)-1].Type != SubagentEventCompleted {
		t.Fatalf("last event should be completed, got %s", events[len(events)-1].Type)
	}
	hasRunning := false
	for _, ev := range events {
		if ev.Type == SubagentEventRunning {
			hasRunning = true
		}
	}
	if !hasRunning {
		t.Fatalf("should emit task_running event for streamed AI message")
	}
}

func TestTaskToolFullUnknownType(t *testing.T) {
	tool := NewTaskToolFull(TaskToolOptions{Runner: nil, PollInterval: time.Millisecond})
	out, err := tool.Run(context.Background(), `{"description":"d","prompt":"p","subagent_type":"nope"}`)
	if err != nil {
		t.Fatalf("unknown type should not error (returns message): %v", err)
	}
	if !strings.Contains(out, "Unknown subagent type") {
		t.Fatalf("expected unknown type message, got %q", out)
	}
}

func TestTaskToolFullBashGating(t *testing.T) {
	tool := NewTaskToolFull(TaskToolOptions{Runner: nil, HostBashAllowed: false, PollInterval: time.Millisecond})
	out, err := tool.Run(context.Background(), `{"description":"d","prompt":"p","subagent_type":"bash"}`)
	if err != nil {
		t.Fatalf("bash gating should not error: %v", err)
	}
	if !strings.Contains(out, "disabled") {
		t.Fatalf("expected bash disabled message, got %q", out)
	}
}

// ── 辅助 ─────────────────────────────────────────────────────────────────────

func intPtr(v int) *int { return &v }
