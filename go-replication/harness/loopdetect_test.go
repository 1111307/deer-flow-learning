package harness

import (
	"reflect"
	"strings"
	"testing"

	"deerflow-go/capability"
)

func bashCall(command string) capability.ToolCall {
	return capability.ToolCall{Name: "bash", Args: `{"command":` + strings.TrimSpace(strJSON(command)) + `}`}
}

// strJSON 把字符串转成 JSON 字面量(简单场景直接拼接)。
func strJSON(s string) string { return `"` + s + `"` }

func TestHashToolCallsOrderIndependent(t *testing.T) {
	a := []capability.ToolCall{
		{Name: "read_file", Args: `{"path":"/tmp/a","start_line":1}`},
		{Name: "bash", Args: `{"command":"ls"}`},
	}
	b := []capability.ToolCall{
		{Name: "bash", Args: `{"command":"ls"}`},
		{Name: "read_file", Args: `{"path":"/tmp/a","start_line":1}`},
	}
	if hashToolCalls(a) != hashToolCalls(b) {
		t.Fatalf("order should not matter: %s vs %s", hashToolCalls(a), hashToolCalls(b))
	}
	c := []capability.ToolCall{
		{Name: "read_file", Args: `{"path":"/tmp/b","start_line":1}`},
	}
	if hashToolCalls(a) == hashToolCalls(c) {
		t.Fatal("different calls should hash differently")
	}
}

func TestStableToolKeyReadFileBucketing(t *testing.T) {
	// 同一 200 行桶内的不同行号 → 同一 key。
	k1 := hashToolCalls([]capability.ToolCall{{Name: "read_file", Args: `{"path":"/tmp/x","start_line":1}`}})
	k150 := hashToolCalls([]capability.ToolCall{{Name: "read_file", Args: `{"path":"/tmp/x","start_line":150}`}})
	if k1 != k150 {
		t.Fatalf("lines 1 and 150 should share bucket: %s vs %s", k1, k150)
	}
	// 跨桶 → 不同 key。
	k201 := hashToolCalls([]capability.ToolCall{{Name: "read_file", Args: `{"path":"/tmp/x","start_line":201}`}})
	if k1 == k201 {
		t.Fatal("line 201 should be a different bucket")
	}
	// 路径不同 → 不同 key。
	kOther := hashToolCalls([]capability.ToolCall{{Name: "read_file", Args: `{"path":"/tmp/y","start_line":1}`}})
	if k1 == kOther {
		t.Fatal("different path should differ")
	}
}

func TestStableToolKeyWriteFileFullArgs(t *testing.T) {
	// write_file 内容敏感:同 path 不同 payload 不塌缩。
	a := hashToolCalls([]capability.ToolCall{{Name: "write_file", Args: `{"path":"/tmp/x","content":"hello"}`}})
	b := hashToolCalls([]capability.ToolCall{{Name: "write_file", Args: `{"path":"/tmp/x","content":"world"}`}})
	if a == b {
		t.Fatal("write_file with different content should differ")
	}
}

func TestTrackAndCheckHardStop(t *testing.T) {
	d := NewLoopDetector()
	for i := 0; i < 5; i++ {
		hardStop, text := d.TrackAndCheck("t1", "r1", []capability.ToolCall{bashCall("echo hi")})
		if i < 4 {
			if hardStop {
				t.Fatalf("expected no hard stop at call %d", i+1)
			}
			continue
		}
		if !hardStop {
			t.Fatal("expected hard stop at 5th identical call")
		}
		if text != loopHardStopMsg {
			t.Fatalf("unexpected hard stop text: %q", text)
		}
	}
}

func TestTrackAndCheckQueuesWarning(t *testing.T) {
	d := NewLoopDetector()
	for i := 0; i < 3; i++ {
		if hardStop, _ := d.TrackAndCheck("t1", "r1", []capability.ToolCall{bashCall("echo hi")}); hardStop {
			t.Fatalf("unexpected hard stop at call %d", i+1)
		}
	}
	warnings := d.DrainPendingWarnings("t1", "r1")
	if len(warnings) != 1 || warnings[0] != loopWarningMsg {
		t.Fatalf("expected one loop warning, got %v", warnings)
	}
	// 已排空。
	if got := d.DrainPendingWarnings("t1", "r1"); len(got) != 0 {
		t.Fatalf("expected empty after drain, got %v", got)
	}
}

func TestTrackAndCheckFreqHardStop(t *testing.T) {
	d := NewLoopDetector()
	for i := 0; i < 50; i++ {
		cmd := "echo step-" + string(rune('a'+i%26))
		hardStop, text := d.TrackAndCheck("t1", "r1", []capability.ToolCall{bashCall(cmd)})
		if i < 49 {
			if hardStop {
				t.Fatalf("unexpected hard stop at %d", i+1)
			}
			continue
		}
		if !hardStop {
			t.Fatal("expected per-tool frequency hard stop at 50")
		}
		if text != "[FORCED STOP] Tool bash called 50 times — exceeded the per-tool safety limit. Producing final answer with results collected so far." {
			t.Fatalf("unexpected freq hard stop text: %q", text)
		}
	}
}

func TestTrackAndCheckPerThreadIsolation(t *testing.T) {
	d := NewLoopDetector()
	// thread t1 累积 3 次 → 触发警告;thread t2 只有 1 次 → 无警告。
	for i := 0; i < 3; i++ {
		d.TrackAndCheck("t1", "r1", []capability.ToolCall{bashCall("echo hi")})
	}
	d.TrackAndCheck("t2", "r1", []capability.ToolCall{bashCall("echo hi")})

	if got := d.DrainPendingWarnings("t1", "r1"); len(got) != 1 {
		t.Fatalf("t1 should have one warning, got %v", got)
	}
	if got := d.DrainPendingWarnings("t2", "r1"); len(got) != 0 {
		t.Fatalf("t2 should have no warning, got %v", got)
	}
}

func TestLoopDetectorLRUEviction(t *testing.T) {
	d := NewLoopDetectorWithConfig(LoopDetectionConfig{
		MaxTrackedThreads: 2,
		WarnThreshold:     3,
		HardLimit:         5,
	})
	d.TrackAndCheck("t1", "r1", []capability.ToolCall{bashCall("echo 1")})
	d.TrackAndCheck("t2", "r1", []capability.ToolCall{bashCall("echo 2")})
	d.TrackAndCheck("t3", "r1", []capability.ToolCall{bashCall("echo 3")}) // 触发淘汰 t1

	d.mu.Lock()
	_, t1Exists := d.history["t1"]
	_, t3Exists := d.history["t3"]
	d.mu.Unlock()
	if t1Exists {
		t.Fatal("t1 should be evicted by LRU")
	}
	if !t3Exists {
		t.Fatal("t3 should be tracked")
	}
}

func TestLoopDetectorReset(t *testing.T) {
	d := NewLoopDetector()
	for i := 0; i < 3; i++ {
		d.TrackAndCheck("t1", "r1", []capability.ToolCall{bashCall("echo hi")})
	}
	d.Reset("t1")
	if got := d.DrainPendingWarnings("t1", "r1"); len(got) != 0 {
		t.Fatalf("reset should clear pending, got %v", got)
	}
	d.mu.Lock()
	_, exists := d.history["t1"]
	d.mu.Unlock()
	if exists {
		t.Fatal("reset should clear history")
	}
}

func TestBuildHardStopMessage(t *testing.T) {
	last := capability.Message{Role: "assistant", Content: "thinking...", ToolCalls: []capability.ToolCall{{ID: "c1", Name: "bash", Args: "{}"}}}
	got := BuildHardStopMessage(last, loopHardStopMsg)
	if got.Role != "assistant" {
		t.Fatalf("role: %q", got.Role)
	}
	if len(got.ToolCalls) != 0 {
		t.Fatalf("tool calls should be cleared: %v", got.ToolCalls)
	}
	if !strings.HasPrefix(got.Content, "thinking...\n\n") {
		t.Fatalf("content should append hard stop text: %q", got.Content)
	}
	// 空内容当作 None:直接返回文本。
	if got := BuildHardStopMessage(capability.Message{Role: "assistant"}, "stop"); got.Content != "stop" {
		t.Fatalf("empty content: %q", got.Content)
	}
}

func TestFormatWarnings(t *testing.T) {
	got := FormatWarnings([]string{"a", "b", "a"})
	if got != "a\n\nb" {
		t.Fatalf("dedupe+join: %q", got)
	}
}

func TestCompatShims(t *testing.T) {
	d := NewLoopDetector()
	for i := 0; i < 3; i++ {
		d.AfterModel([]capability.ToolCall{bashCall("echo hi")})
	}
	if got := d.DrainPending(); !reflect.DeepEqual(got, []string{loopWarningMsg}) {
		t.Fatalf("compat shim: got %v", got)
	}
}
