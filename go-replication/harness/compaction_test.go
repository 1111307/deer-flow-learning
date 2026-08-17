package harness

import (
	"context"
	"strings"
	"testing"

	"deerflow-go/capability"
)

func msgs(roles ...string) []capability.Message {
	out := make([]capability.Message, 0, len(roles))
	for i, r := range roles {
		out = append(out, capability.Message{Role: r, Content: "m" + string(rune('a'+i))})
	}
	return out
}

func TestMaybeCompact(t *testing.T) {
	a := New(nil, nil, nil)
	a.Compaction = SummarizationConfig{
		Enabled: true,
		Trigger: []ContextSize{{Type: ContextMessages, Value: 4}},
		Keep:    ContextSize{Type: ContextMessages, Value: 2},
	}
	var summarized []capability.Message
	a.Summarizer = func(_ context.Context, ms []capability.Message) (string, error) {
		summarized = append(summarized, ms...)
		return "SUMMARY", nil
	}

	thread := &Thread{ID: "t1", Messages: msgs("user", "assistant", "user", "assistant", "user")}
	if err := a.maybeCompact(context.Background(), thread); err != nil {
		t.Fatalf("maybeCompact: %v", err)
	}

	// 摘要掉前 3 条,保留后 2 条,加一条 summary。
	if len(thread.Messages) != 3 {
		t.Fatalf("expected 3 messages after compaction, got %d", len(thread.Messages))
	}
	if thread.Messages[0].Name != "summary" || !strings.Contains(thread.Messages[0].Content, "SUMMARY") {
		t.Fatalf("expected summary head message, got %+v", thread.Messages[0])
	}
	if len(summarized) != 3 {
		t.Fatalf("expected 3 messages summarized, got %d", len(summarized))
	}
}

func TestMaybeCompactNotTriggered(t *testing.T) {
	a := New(nil, nil, nil)
	a.Compaction = SummarizationConfig{
		Enabled: true,
		Trigger: []ContextSize{{Type: ContextMessages, Value: 100}},
		Keep:    ContextSize{Type: ContextMessages, Value: 2},
	}
	thread := &Thread{ID: "t1", Messages: msgs("user", "assistant")}
	if err := a.maybeCompact(context.Background(), thread); err != nil {
		t.Fatalf("maybeCompact: %v", err)
	}
	if len(thread.Messages) != 2 {
		t.Fatalf("compaction should not trigger below threshold, got %d", len(thread.Messages))
	}
}

func TestCompactionFilterMessagesForMemory(t *testing.T) {
	in := []capability.Message{
		{Role: "user", Content: "hello"},
		{Role: "assistant", Content: "hi there"},
		{Role: "assistant", Content: "", ToolCalls: []capability.ToolCall{{ID: "1", Name: "bash"}}},
		{Role: "tool", ToolCallID: "1", Content: "output"},
		{Role: "user", Content: "thanks"},
	}
	out := filterMessagesForMemory(in)
	// 保留 2 条 user + 1 条无 tool_calls 的 assistant;跳过带 tool_calls 的 assistant 与 tool。
	if len(out) != 3 {
		t.Fatalf("expected 3 filtered messages, got %d", len(out))
	}
	if out[0].Role != "user" || out[1].Role != "assistant" || out[2].Role != "user" {
		t.Fatalf("unexpected filtered sequence: %+v", out)
	}
}

func TestFilterMessagesUploadBlock(t *testing.T) {
	in := []capability.Message{
		{Role: "user", Content: "<uploaded_files>file.pdf</uploaded_files>rest of msg"},
		{Role: "assistant", Content: "ok"},
	}
	out := filterMessagesForMemory(in)
	if len(out) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(out))
	}
	if strings.Contains(out[0].Content, "uploaded_files") {
		t.Fatalf("upload block should be stripped, got %q", out[0].Content)
	}
	// 纯上传块(剥离后为空)→ 跳过并连带跳过下一条 AI。
	in2 := []capability.Message{
		{Role: "user", Content: "<uploaded_files>file.pdf</uploaded_files>"},
		{Role: "assistant", Content: "got it"},
	}
	out2 := filterMessagesForMemory(in2)
	if len(out2) != 0 {
		t.Fatalf("pure upload block should be skipped entirely, got %d", len(out2))
	}
}

func TestCompactionDetectSignals(t *testing.T) {
	if !detectCorrection([]capability.Message{{Role: "user", Content: "that's wrong, redo it"}}) {
		t.Fatal("expected correction detected")
	}
	if detectCorrection([]capability.Message{{Role: "user", Content: "all good"}}) {
		t.Fatal("unexpected correction")
	}
	if !detectReinforcement([]capability.Message{{Role: "user", Content: "yes, exactly right!"}}) {
		t.Fatal("expected reinforcement detected")
	}
	if detectReinforcement([]capability.Message{{Role: "user", Content: "meh"}}) {
		t.Fatal("unexpected reinforcement")
	}
}

func TestSkillRescue(t *testing.T) {
	// 构造一条读 skill 的 assistant + 配对 tool 消息。
	skillsRoot := "/mnt/skills"
	messages := []capability.Message{
		{Role: "assistant", ToolCalls: []capability.ToolCall{
			{ID: "s1", Name: "read_file", Args: `{"path":"/mnt/skills/foo/SKILL.md"}`},
		}},
		{Role: "tool", ToolCallID: "s1", Name: "read_file", Content: "SKILL CONTENT"},
		{Role: "user", Content: "do something"},
	}
	bundles := findSkillBundles(messages, skillsRoot, nil, defaultTokenCounter)
	if len(bundles) != 1 {
		t.Fatalf("expected 1 skill bundle, got %d", len(bundles))
	}
	if bundles[0].skillKey != "/mnt/skills/foo/SKILL.md" {
		t.Fatalf("unexpected skill key: %q", bundles[0].skillKey)
	}

	// 救援:count 预算内选中 bundle,把 AI + tool 从摘要侧救到保留侧。
	a := New(nil, nil, nil)
	a.Compaction = DefaultSummarizationConfig()
	toSummarize, preserved := a.partitionWithSkillRescue(messages, 2)
	// AI + tool 被救出摘要侧,user 本就在保留侧 → 摘要侧清空,保留侧 3 条。
	if len(toSummarize) != 0 {
		t.Fatalf("expected empty summarize side after rescue, got %+v", toSummarize)
	}
	if len(preserved) != 3 {
		t.Fatalf("expected 3 preserved (AI + tool + user), got %d", len(preserved))
	}
	if preserved[0].Role != "assistant" || preserved[1].Role != "tool" || preserved[2].Role != "user" {
		t.Fatalf("unexpected preserved order: %+v", preserved)
	}
}
