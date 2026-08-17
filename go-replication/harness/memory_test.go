package harness

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"deerflow-go/capability"
)

// ── 防抖队列 ────────────────────────────────────────────────────────────────

func TestMemoryQueueDebounceMergesAndOR(t *testing.T) {
	var processed []*ConversationContext
	q := NewMemoryQueue(20*time.Millisecond, func(batch []*ConversationContext) {
		processed = append(processed, batch...)
	})
	defer q.Clear()

	// 同一 (thread,user,agent) 三次入队,窗口内合并;信号 OR 合并。
	q.Add(&ConversationContext{ThreadID: "t1", UserID: "u1", AgentName: "a", Correction: false})
	q.Add(&ConversationContext{ThreadID: "t1", UserID: "u1", AgentName: "a", Correction: true})
	q.Add(&ConversationContext{ThreadID: "t1", UserID: "u1", AgentName: "a", Reinforcement: true})

	// 不同 key 不合并。
	q.Add(&ConversationContext{ThreadID: "t2", UserID: "u2", AgentName: "a", Correction: false})

	time.Sleep(60 * time.Millisecond)

	if len(processed) != 2 {
		t.Fatalf("should debounce into 2 contexts, got %d", len(processed))
	}
	var t1 *ConversationContext
	for _, c := range processed {
		if c.ThreadID == "t1" {
			t1 = c
		}
	}
	if t1 == nil {
		t.Fatalf("t1 context missing")
	}
	if !t1.Correction || !t1.Reinforcement {
		t.Fatalf("OR merge failed: Correction=%v Reinforcement=%v", t1.Correction, t1.Reinforcement)
	}
	if q.PendingCount() != 0 {
		t.Fatalf("queue should be empty after processing, got %d", q.PendingCount())
	}
}

func TestMemoryQueueFlushAndClear(t *testing.T) {
	var processed []*ConversationContext
	q := NewMemoryQueue(time.Hour, func(batch []*ConversationContext) {
		processed = append(processed, batch...)
	})
	defer q.Clear()

	q.Add(&ConversationContext{ThreadID: "t1", UserID: "u1"})
	if q.PendingCount() != 1 {
		t.Fatalf("expected 1 pending, got %d", q.PendingCount())
	}
	q.Flush() // 强制同步处理
	if len(processed) != 1 {
		t.Fatalf("flush should process queue, got %d", len(processed))
	}
	if q.PendingCount() != 0 {
		t.Fatalf("after flush pending should be 0, got %d", q.PendingCount())
	}

	q.Add(&ConversationContext{ThreadID: "t2", UserID: "u2"})
	q.Clear() // 清空不处理
	if q.PendingCount() != 0 {
		t.Fatalf("after clear pending should be 0, got %d", q.PendingCount())
	}
	if len(processed) != 1 {
		t.Fatalf("clear should not process, got %d processed", len(processed))
	}
}

func TestMemoryQueueDisabled(t *testing.T) {
	var processed []*ConversationContext
	q := NewMemoryQueue(10*time.Millisecond, func(batch []*ConversationContext) {
		processed = append(processed, batch...)
	})
	defer q.Clear()
	q.SetEnabled(false)

	q.Add(&ConversationContext{ThreadID: "t1", UserID: "u1"})
	time.Sleep(30 * time.Millisecond)
	if len(processed) != 0 {
		t.Fatalf("disabled queue should not process, got %d", len(processed))
	}
}

// ── 存储 ─────────────────────────────────────────────────────────────────────

func newTestStorage(t *testing.T) (*FileMemoryStorage, MemoryConfig) {
	t.Helper()
	cfg := DefaultMemoryConfig()
	cfg.BaseDir = t.TempDir()
	return NewFileMemoryStorage(cfg), cfg
}

func TestFileMemoryStorageSaveLoadRoundtrip(t *testing.T) {
	s, _ := newTestStorage(t)
	data := CreateEmptyMemory()
	data["facts"] = []any{map[string]any{"id": "fact_1", "content": "hello", "confidence": 0.9}}

	ok, err := s.Save(data, "agentA", "user1")
	if err != nil || !ok {
		t.Fatalf("Save failed: ok=%v err=%v", ok, err)
	}

	// 文件应存在于 per-user per-agent 路径(agent 名小写)。
	expected := filepath.Join(s.baseDir, "users", "user1", "agents", "agenta", "memory.json")
	if _, err := os.Stat(expected); err != nil {
		t.Fatalf("expected file at %s: %v", expected, err)
	}

	loaded, err := s.Load("agentA", "user1")
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	facts := loaded["facts"].([]any)
	if len(facts) != 1 {
		t.Fatalf("expected 1 fact, got %d", len(facts))
	}
	// lastUpdated 应该被 Save 注入。
	if _, ok := loaded["lastUpdated"].(string); !ok {
		t.Fatalf("lastUpdated should be injected by Save")
	}
}

func TestFileMemoryStoragePerUserIsolation(t *testing.T) {
	s, _ := newTestStorage(t)
	d1 := CreateEmptyMemory()
	d1["facts"] = []any{map[string]any{"id": "f1", "content": "u1 fact"}}
	s.Save(d1, "", "user1")

	d2 := CreateEmptyMemory()
	d2["facts"] = []any{map[string]any{"id": "f2", "content": "u2 fact"}}
	s.Save(d2, "", "user2")

	got1, _ := s.Load("", "user1")
	got2, _ := s.Load("", "user2")
	if got1["facts"].([]any)[0].(map[string]any)["content"] != "u1 fact" {
		t.Fatalf("user1 memory leaked")
	}
	if got2["facts"].([]any)[0].(map[string]any)["content"] != "u2 fact" {
		t.Fatalf("user2 memory leaked")
	}
}

func TestFileMemoryStorageMtimeCacheReload(t *testing.T) {
	s, _ := newTestStorage(t)
	s.Save(CreateEmptyMemory(), "", "user1")

	// 首次 Load 建立缓存。
	_, _ = s.Load("", "user1")

	// 外部写文件(绕过缓存)。
	external := CreateEmptyMemory()
	external["facts"] = []any{map[string]any{"id": "external", "content": "external"}}
	blob, _ := json.Marshal(external)
	path := filepath.Join(s.baseDir, "users", "user1", "memory.json")
	if err := os.WriteFile(path, blob, 0o644); err != nil {
		t.Fatalf("write external: %v", err)
	}

	// Load 应通过 mtime 检测到变化并重载。
	loaded, _ := s.Load("", "user1")
	if loaded["facts"].([]any)[0].(map[string]any)["id"] != "external" {
		t.Fatalf("mtime cache should invalidate on external change")
	}
}

func TestFileMemoryStorageInvalidAgentName(t *testing.T) {
	s, _ := newTestStorage(t)
	if _, err := s.Load("bad/name", "user1"); err == nil {
		t.Fatalf("invalid agent name should error (path traversal)")
	}
	if _, err := s.Save(CreateEmptyMemory(), "", "bad name"); err == nil {
		t.Fatalf("invalid user id should error")
	}
}

func TestFileMemoryStorageCorruptFileReturnsEmpty(t *testing.T) {
	s, _ := newTestStorage(t)
	dir := filepath.Join(s.baseDir, "users", "user1")
	os.MkdirAll(dir, 0o755)
	os.WriteFile(filepath.Join(dir, "memory.json"), []byte("{ not json"), 0o644)

	loaded, err := s.Load("", "user1")
	if err != nil {
		t.Fatalf("corrupt file should return empty without error, got %v", err)
	}
	if loaded["version"] != "1.0" {
		t.Fatalf("corrupt file should yield empty memory structure")
	}
}

// ── 消息处理(委托到 compaction.go 的实现)────────────────────────────────────

func TestFilterMessagesForMemory(t *testing.T) {
	msgs := []capability.Message{
		{Role: "user", Content: "hello"},
		{Role: "assistant", ToolCalls: []capability.ToolCall{{ID: "1", Name: "bash"}}},
		{Role: "assistant", Content: "final answer"},
		{Role: "user", Content: "<uploaded_files>f.txt</uploaded_files>"},
		{Role: "assistant", Content: "received"},
		{Role: "user", Content: "ok now"},
	}
	filtered := FilterMessagesForMemory(msgs)
	if len(filtered) != 3 {
		t.Fatalf("expected 3 messages, got %d: %+v", len(filtered), filtered)
	}
}

func TestDetectCorrectionAndReinforcement(t *testing.T) {
	corr := []capability.Message{{Role: "user", Content: "no, that's wrong, try again"}}
	if !DetectCorrection(corr) {
		t.Fatalf("should detect correction")
	}
	rein := []capability.Message{{Role: "user", Content: "yes exactly, that's right"}}
	if !DetectReinforcement(rein) {
		t.Fatalf("should detect reinforcement")
	}
	neutral := []capability.Message{{Role: "user", Content: "what time is it"}}
	if DetectCorrection(neutral) || DetectReinforcement(neutral) {
		t.Fatalf("neutral message should not trigger signals")
	}
}

// ── 更新器 ───────────────────────────────────────────────────────────────────

type fakeMemoryModel struct {
	response string
	calls    int32
}

func (f *fakeMemoryModel) Invoke(_ context.Context, prompt string) (string, error) {
	atomic.AddInt32(&f.calls, 1)
	if !strings.Contains(prompt, "Current Memory State") {
		panic("prompt should contain memory state")
	}
	return f.response, nil
}

func newTestUpdater(t *testing.T, response string) (*MemoryUpdater, MemoryConfig) {
	t.Helper()
	cfg := DefaultMemoryConfig()
	cfg.BaseDir = t.TempDir()
	cfg.Enabled = true
	cfg.FactConfidenceThreshold = 0.7
	SetMemoryConfig(cfg)
	u := &MemoryUpdater{
		Model:   &fakeMemoryModel{response: response},
		Storage: NewFileMemoryStorage(cfg),
	}
	return u, cfg
}

func TestMemoryUpdaterAppliesUpdate(t *testing.T) {
	response := `{
		"user": {"workContext": {"summary": "Backend engineer", "shouldUpdate": true}},
		"history": {"recentMonths": {"summary": "working on Go", "shouldUpdate": true}},
		"newFacts": [
			{"content": "prefers Go", "category": "preference", "confidence": 0.9},
			{"content": "low confidence fact", "category": "context", "confidence": 0.2}
		],
		"factsToRemove": []
	}`
	u, _ := newTestUpdater(t, response)

	msgs := []capability.Message{
		{Role: "user", Content: "I work on backend systems"},
		{Role: "assistant", Content: "got it"},
	}
	ok := u.UpdateMemory(context.Background(), msgs, "thread1", "", "user1", false, false)
	if !ok {
		t.Fatalf("UpdateMemory should succeed")
	}

	mem := GetMemoryData("", "user1")
	work := mem["user"].(map[string]any)["workContext"].(map[string]any)
	if work["summary"] != "Backend engineer" {
		t.Fatalf("workContext not updated: %v", work)
	}
	facts := mem["facts"].([]any)
	if len(facts) != 1 {
		t.Fatalf("expected 1 fact (low confidence dropped), got %d", len(facts))
	}
	if facts[0].(map[string]any)["content"] != "prefers Go" {
		t.Fatalf("fact content wrong: %v", facts[0])
	}
}

func TestMemoryUpdaterFactDedupeAndRemove(t *testing.T) {
	// 预置一条 fact,再让模型删除它并添加一条新的。
	_, cfg := newTestUpdater(t, "")
	storage := NewFileMemoryStorage(cfg)
	seed := CreateEmptyMemory()
	seed["facts"] = []any{map[string]any{"id": "fact_old", "content": "old fact", "confidence": 0.9}}
	storage.Save(seed, "", "user1")
	SetMemoryStorage(storage)
	defer ResetMemoryStorage()

	response := `{
		"user": {}, "history": {},
		"newFacts": [{"content": "new fact", "category": "context", "confidence": 0.8}],
		"factsToRemove": ["fact_old"]
	}`
	u := &MemoryUpdater{Model: &fakeMemoryModel{response: response}, Storage: storage}

	ok := u.UpdateMemory(context.Background(), []capability.Message{{Role: "user", Content: "hi"}}, "t1", "", "user1", false, false)
	if !ok {
		t.Fatalf("update should succeed")
	}
	mem, _ := storage.Load("", "user1")
	facts := mem["facts"].([]any)
	if len(facts) != 1 || facts[0].(map[string]any)["content"] != "new fact" {
		t.Fatalf("expected old removed + new added, got %v", facts)
	}
}

func TestMemoryUpdaterParseThinkingTrace(t *testing.T) {
	// 响应被 thinking trace 包裹,解析器应提取第一个有效 JSON。
	response := "Let me think...\n```json\n{\"user\": {}, \"history\": {}, \"newFacts\": [], \"factsToRemove\": []}\n```\nDone"
	u, _ := newTestUpdater(t, response)
	ok := u.UpdateMemory(context.Background(), []capability.Message{{Role: "user", Content: "hi"}}, "t1", "", "user1", false, false)
	if !ok {
		t.Fatalf("should parse JSON wrapped in prose")
	}
}

func TestMemoryUpdaterMalformedResponseFails(t *testing.T) {
	u, _ := newTestUpdater(t, "this is not json at all")
	ok := u.UpdateMemory(context.Background(), []capability.Message{{Role: "user", Content: "hi"}}, "t1", "", "user1", false, false)
	if ok {
		t.Fatalf("malformed response should fail")
	}
}

func TestNormalizeMemoryUpdateFact(t *testing.T) {
	// bool confidence 非法。
	if got := normalizeMemoryUpdateFact(map[string]any{"content": "x", "confidence": true}); got != nil {
		t.Fatalf("bool confidence should be rejected")
	}
	// 空 content 非法。
	if got := normalizeMemoryUpdateFact(map[string]any{"content": "  ", "confidence": 0.5}); got != nil {
		t.Fatalf("empty content should be rejected")
	}
	// 字符串置信度可解析。
	got := normalizeMemoryUpdateFact(map[string]any{"content": "x", "confidence": "0.8"})
	if got == nil || got["confidence"].(float64) != 0.8 {
		t.Fatalf("string confidence should parse: %v", got)
	}
}

func TestFactCRUD(t *testing.T) {
	_, cfg := newTestUpdater(t, "")
	storage := NewFileMemoryStorage(cfg)
	SetMemoryStorage(storage)
	defer ResetMemoryStorage()

	mem, err := CreateMemoryFact("likes Go", "preference", 0.9, "", "user1")
	if err != nil {
		t.Fatalf("CreateMemoryFact: %v", err)
	}
	facts := mem["facts"].([]any)
	if len(facts) != 1 {
		t.Fatalf("expected 1 fact")
	}
	id := facts[0].(map[string]any)["id"].(string)

	content := "loves Rust"
	mem, err = UpdateMemoryFact(id, &content, nil, nil, "", "user1")
	if err != nil {
		t.Fatalf("UpdateMemoryFact: %v", err)
	}
	if mem["facts"].([]any)[0].(map[string]any)["content"] != "loves Rust" {
		t.Fatalf("fact not updated")
	}

	mem, err = DeleteMemoryFact(id, "", "user1")
	if err != nil {
		t.Fatalf("DeleteMemoryFact: %v", err)
	}
	if len(mem["facts"].([]any)) != 0 {
		t.Fatalf("fact not deleted")
	}
}

func TestValidateConfidence(t *testing.T) {
	if _, err := validateConfidence(1.5); err == nil {
		t.Fatalf("out of range confidence should error")
	}
	if _, err := validateConfidence(-0.1); err == nil {
		t.Fatalf("negative confidence should error")
	}
	if v, err := validateConfidence(0.7); err != nil || v != 0.7 {
		t.Fatalf("valid confidence should pass")
	}
}
