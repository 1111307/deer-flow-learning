package harness

import (
	"context"
	"testing"
	"time"

	"deerflow-go/capability"
	"deerflow-go/runtime"
)

func TestRunAgentSuccess(t *testing.T) {
	m := &scriptedModel{responses: []capability.ChatResponse{
		{Message: capability.Message{Role: "assistant", Content: "all done"}},
	}}
	a := New(m, map[string]capability.Tool{}, nil)
	thread := &Thread{ID: "t1", Messages: []capability.Message{{Role: "user", Content: "seed"}}}

	store := &fakeRunStore{}
	mgr := NewRunManager(store)
	rec, err := mgr.CreateOrReject("t1", "lead_agent", DisconnectCancel, "reject", "", "", nil, nil)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	bridge := runtime.NewStreamBridge(256)

	RunAgent(bridge, mgr, rec, a, thread, "hello")

	if rec.Status != RunSuccess {
		t.Fatalf("expected success, got %s (err=%q)", rec.Status, rec.Err)
	}
	// 状态持久化到 store。
	if got := store.statuses[rec.RunID]; got != RunSuccess {
		t.Fatalf("expected store status success, got %s", got)
	}
}

// slowTool 每次执行 sleep,让 run 持续足够久以便 abort 能在中途生效。
type slowTool struct{}

func (slowTool) Name() string        { return "echo" }
func (slowTool) Description() string { return "slow" }
func (slowTool) Run(_ context.Context, _ string) (string, error) {
	time.Sleep(20 * time.Millisecond)
	return "ok", nil
}

func TestRunAgentAbortInterrupt(t *testing.T) {
	// 模型永远返回工具调用,以便在运行中中断。
	m := &scriptedModel{}
	for i := 0; i < 50; i++ {
		m.responses = append(m.responses, capability.ChatResponse{
			Message: capability.Message{Role: "assistant", ToolCalls: []capability.ToolCall{{ID: "c", Name: "echo", Args: `{}`}}},
		})
	}
	a := New(m, map[string]capability.Tool{"echo": slowTool{}}, nil)
	thread := &Thread{ID: "t1"}

	mgr := NewRunManager(nil)
	rec, _ := mgr.CreateOrReject("t1", "", DisconnectCancel, "reject", "", "", nil, nil)
	bridge := runtime.NewStreamBridge(256)

	// 启动后立即 abort(interrupt 语义)。
	go func() {
		time.Sleep(20 * time.Millisecond)
		rec.Abort()
	}()

	done := make(chan struct{})
	go func() {
		RunAgent(bridge, mgr, rec, a, thread, "go")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("runAgent did not terminate after abort")
	}

	if rec.Status != RunInterrupted {
		t.Fatalf("expected interrupted, got %s", rec.Status)
	}
}

func TestRunAgentRollback(t *testing.T) {
	m := &scriptedModel{}
	for i := 0; i < 50; i++ {
		m.responses = append(m.responses, capability.ChatResponse{
			Message: capability.Message{Role: "assistant", ToolCalls: []capability.ToolCall{{ID: "c", Name: "echo", Args: `{}`}}},
		})
	}
	a := New(m, map[string]capability.Tool{"echo": slowTool{}}, nil)
	pre := []capability.Message{{Role: "user", Content: "before"}}
	thread := &Thread{ID: "t1", Messages: append([]capability.Message(nil), pre...)}

	mgr := NewRunManager(nil)
	rec, _ := mgr.CreateOrReject("t1", "", DisconnectCancel, "rollback", "", "", nil, nil)
	bridge := runtime.NewStreamBridge(256)

	go func() {
		time.Sleep(20 * time.Millisecond)
		rec.AbortAction = "rollback"
		rec.Abort()
	}()

	done := make(chan struct{})
	go func() {
		RunAgent(bridge, mgr, rec, a, thread, "do work")
		close(done)
	}()
	<-done

	if rec.Status != RunError || rec.Err != "Rolled back by user" {
		t.Fatalf("expected error+rollback, got %s (err=%q)", rec.Status, rec.Err)
	}
	// 回滚后 thread.Messages 恢复到 run 前 checkpoint。
	if len(thread.Messages) != len(pre) || thread.Messages[0].Content != "before" {
		t.Fatalf("expected rollback to pre-run checkpoint, got %+v", thread.Messages)
	}
}

// fakeRunStore 最小持久化实现,记录状态。
type fakeRunStore struct {
	rows     map[string]*RunRecord
	statuses map[string]RunStatus
}

func (f *fakeRunStore) ensure() {
	if f.rows == nil {
		f.rows = map[string]*RunRecord{}
		f.statuses = map[string]RunStatus{}
	}
}
func (f *fakeRunStore) Put(runID string, rec *RunRecord) error {
	f.ensure()
	f.rows[runID] = rec
	f.statuses[runID] = rec.Status
	return nil
}
func (f *fakeRunStore) Get(runID string) (*RunRecord, bool, error) {
	f.ensure()
	r, ok := f.rows[runID]
	return r, ok, nil
}
func (f *fakeRunStore) ListByThread(threadID string, limit int) ([]*RunRecord, error) {
	f.ensure()
	var out []*RunRecord
	for _, r := range f.rows {
		if r.ThreadID == threadID {
			out = append(out, r)
		}
	}
	return out, nil
}
func (f *fakeRunStore) UpdateStatus(runID string, status RunStatus, errMsg string) (bool, error) {
	f.ensure()
	f.statuses[runID] = status
	if r, ok := f.rows[runID]; ok {
		r.Status = status
		r.Err = errMsg
	}
	return true, nil
}
func (f *fakeRunStore) ListInflight() ([]*RunRecord, error) {
	f.ensure()
	var out []*RunRecord
	for _, r := range f.rows {
		if isInflight(r.Status) {
			out = append(out, r)
		}
	}
	return out, nil
}

// 确保 fakeRunStore 满足 RunStore 接口。
var _ RunStore = (*fakeRunStore)(nil)
