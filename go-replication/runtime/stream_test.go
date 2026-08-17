package runtime

import (
	"testing"
	"time"
)

// collect 从 channel 收集事件,直到 __end__ 哨兵或超时。
func collect(t *testing.T, ch <-chan StreamEvent, timeout time.Duration) []StreamEvent {
	t.Helper()
	var events []StreamEvent
	deadline := time.After(timeout)
	for {
		select {
		case e, ok := <-ch:
			if !ok {
				return events
			}
			events = append(events, e)
			if e.Event == "__end__" {
				return events
			}
		case <-deadline:
			t.Fatalf("timed out waiting for events; got %d so far", len(events))
			return events
		}
	}
}

func TestPublishSubscribeAndEnd(t *testing.T) {
	b := NewStreamBridge(256)
	b.Publish("r1", "metadata", `{"run_id":"r1"}`)
	b.Publish("r1", "values", `[{"role":"user"}]`)
	b.PublishEnd("r1")

	events := collect(t, b.Subscribe("r1", ""), 2*time.Second)
	if len(events) < 3 {
		t.Fatalf("expected at least 3 events (2 + end), got %d", len(events))
	}
	if events[0].Event != "metadata" || events[1].Event != "values" {
		t.Fatalf("unexpected replay order: %+v", events)
	}
	if last := events[len(events)-1]; last.Event != "__end__" {
		t.Fatalf("expected __end__ sentinel, got %+v", last)
	}
}

func TestLastEventIDReplay(t *testing.T) {
	b := NewStreamBridge(256)
	for i := 0; i < 5; i++ {
		b.Publish("r2", "values", "e")
	}
	b.PublishEnd("r2")

	// 先取一个事件 id,再从其之后重放。
	all := collect(t, b.Subscribe("r2", ""), 2*time.Second)
	if len(all) < 2 {
		t.Fatalf("expected at least 2 events, got %d", len(all))
	}
	lastID := all[0].ID // 重放从第二个事件开始

	replayed := collect(t, b.Subscribe("r2", lastID), 2*time.Second)
	if len(replayed) < 4 { // 5 total - 1 before lastID = 4 + end
		t.Fatalf("expected 4 events after %s, got %d", lastID, len(replayed))
	}
}

func TestBoundedBufferEviction(t *testing.T) {
	b := NewStreamBridge(3)
	for i := 0; i < 10; i++ {
		b.Publish("r3", "values", "x")
	}
	b.PublishEnd("r3")

	// 缓冲只保留最近 3 个事件(不含 end),重放应从最早保留事件开始。
	events := collect(t, b.Subscribe("r3", ""), 2*time.Second)
	dataCount := 0
	for _, e := range events {
		if e.Event == "values" {
			dataCount++
		}
	}
	if dataCount != 3 {
		t.Fatalf("expected 3 retained events, got %d", dataCount)
	}
}

func TestHeartbeat(t *testing.T) {
	b := NewStreamBridge(256)
	b.Publish("r4", "metadata", "x")

	ch := b.SubscribeHeartbeat("r4", "", 50*time.Millisecond)
	// 第一个事件是 metadata,之后应收到至少一个 heartbeat(因为不再有事件)。
	deadline := time.After(500 * time.Millisecond)
	sawHeartbeat := false
	for {
		select {
		case e, ok := <-ch:
			if !ok {
				return
			}
			if e.Event == "__heartbeat__" {
				sawHeartbeat = true
				return
			}
		case <-deadline:
			if !sawHeartbeat {
				t.Fatal("expected heartbeat event")
			}
			return
		}
	}
}

func TestCleanupAfter(t *testing.T) {
	b := NewStreamBridge(256)
	b.Publish("r5", "metadata", "x")
	b.CleanupAfter("r5", 20*time.Millisecond)
	time.Sleep(50 * time.Millisecond)

	b.mu.Lock()
	_, exists := b.streams["r5"]
	b.mu.Unlock()
	if exists {
		t.Fatal("expected stream to be cleaned up")
	}
}
