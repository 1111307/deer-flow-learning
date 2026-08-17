package provider

import (
	"strings"
	"testing"
)

// nget 在测试里安全导航嵌套 map。
func nget(m map[string]any, keys ...string) any {
	var cur any = m
	for _, k := range keys {
		mm, ok := cur.(map[string]any)
		if !ok {
			return nil
		}
		cur = mm[k]
	}
	return cur
}

// OpenAI 兼容方言:thinking 嵌套在 extra_body.thinking.type(factory.py:142-148)。
func TestCreateChatModelOpenAICompatThinking(t *testing.T) {
	cfg := &AppConfig{Models: []ModelConfig{{
		Name:                    "deepseek-reasoner",
		Use:                     UseChatOpenAI,
		SupportsThinking:        true,
		SupportsReasoningEffort: true,
		WhenThinkingEnabled: map[string]any{
			"extra_body": map[string]any{"thinking": map[string]any{"type": "enabled"}},
		},
	}}}

	on, err := CreateChatModel("deepseek-reasoner", true, cfg, nil)
	if err != nil {
		t.Fatalf("enable: %v", err)
	}
	if got := nget(on.Settings, "extra_body", "thinking", "type"); got != "enabled" {
		t.Errorf("enabled extra_body.thinking.type = %v, want enabled", got)
	}

	off, err := CreateChatModel("deepseek-reasoner", false, cfg, nil)
	if err != nil {
		t.Fatalf("disable: %v", err)
	}
	if got := nget(off.Settings, "extra_body", "thinking", "type"); got != "disabled" {
		t.Errorf("disabled extra_body.thinking.type = %v, want disabled", got)
	}
	if got := off.Settings["reasoning_effort"]; got != "minimal" {
		t.Errorf("disabled reasoning_effort = %v, want minimal", got)
	}
}

// Anthropic 方言:thinking 是直接构造参数(factory.py:155-157)。
func TestCreateChatModelAnthropicThinking(t *testing.T) {
	cfg := &AppConfig{Models: []ModelConfig{{
		Name:             "claude",
		Use:              UseChatAnthropic,
		SupportsThinking: true,
		Thinking:         map[string]any{"type": "enabled", "budget_tokens": 1000},
	}}}

	on, err := CreateChatModel("claude", true, cfg, nil)
	if err != nil {
		t.Fatalf("enable: %v", err)
	}
	if got := nget(on.Settings, "thinking", "type"); got != "enabled" {
		t.Errorf("enabled thinking.type = %v", got)
	}
	if got := nget(on.Settings, "thinking", "budget_tokens"); got != 1000 {
		t.Errorf("budget_tokens = %v", got)
	}

	off, err := CreateChatModel("claude", false, cfg, nil)
	if err != nil {
		t.Fatalf("disable: %v", err)
	}
	if got := nget(off.Settings, "thinking", "type"); got != "disabled" {
		t.Errorf("disabled thinking.type = %v", got)
	}
}

// vLLM 方言:extra_body.chat_template_kwargs 开关思考(factory.py:149-154)。
func TestCreateChatModelVllmThinking(t *testing.T) {
	cfg := &AppConfig{Models: []ModelConfig{{
		Name:             "qwen",
		Use:              UseVllmChatModel,
		SupportsThinking: true,
		WhenThinkingEnabled: map[string]any{
			"extra_body": map[string]any{"chat_template_kwargs": map[string]any{"thinking": true}},
		},
	}}}

	on, err := CreateChatModel("qwen", true, cfg, nil)
	if err != nil {
		t.Fatalf("enable: %v", err)
	}
	if got := nget(on.Settings, "extra_body", "chat_template_kwargs", "thinking"); got != true {
		t.Errorf("enabled chat_template_kwargs.thinking = %v", got)
	}

	off, err := CreateChatModel("qwen", false, cfg, nil)
	if err != nil {
		t.Fatalf("disable: %v", err)
	}
	if got := nget(off.Settings, "extra_body", "chat_template_kwargs", "thinking"); got != false {
		t.Errorf("disabled chat_template_kwargs.thinking = %v", got)
	}
}

// Codex 方言:reasoning_effort(factory.py:165-179)。
func TestCreateChatModelCodexReasoningEffort(t *testing.T) {
	cfg := &AppConfig{Models: []ModelConfig{{
		Name:                    "gpt-codex",
		Use:                     UseCodexChatModel,
		SupportsThinking:        true,
		SupportsReasoningEffort: true,
		Settings:                map[string]any{"max_tokens": 1000},
	}}}

	on, err := CreateChatModel("gpt-codex", true, cfg, nil)
	if err != nil {
		t.Fatalf("enable: %v", err)
	}
	if got := on.Settings["reasoning_effort"]; got != "medium" {
		t.Errorf("enabled reasoning_effort = %v, want medium", got)
	}
	if _, ok := on.Settings["max_tokens"]; ok {
		t.Error("max_tokens must be dropped for Codex")
	}

	off, err := CreateChatModel("gpt-codex", false, cfg, nil)
	if err != nil {
		t.Fatalf("disable: %v", err)
	}
	if got := off.Settings["reasoning_effort"]; got != "none" {
		t.Errorf("disabled reasoning_effort = %v, want none", got)
	}

	// 显式 reasoning_effort 透传。
	explicit, err := CreateChatModel("gpt-codex", true, cfg, map[string]any{"reasoning_effort": "high"})
	if err != nil {
		t.Fatalf("explicit: %v", err)
	}
	if got := explicit.Settings["reasoning_effort"]; got != "high" {
		t.Errorf("explicit reasoning_effort = %v, want high", got)
	}
}

// supports_thinking=false 却要求开启 → 报错(factory.py:134-135)。
func TestCreateChatModelThinkingNotSupported(t *testing.T) {
	cfg := &AppConfig{Models: []ModelConfig{{
		Name:             "m",
		Use:              UseChatAnthropic,
		SupportsThinking: false,
		Thinking:         map[string]any{"type": "enabled"},
	}}}
	if _, err := CreateChatModel("m", true, cfg, nil); err == nil || !strings.Contains(err.Error(), "does not support thinking") {
		t.Errorf("expected supports_thinking error, got %v", err)
	}
}

// 三个 LangChain 默认值补丁:stream_usage / stream_chunk_timeout / deep_merge。
func TestLangChainDefaultPatches(t *testing.T) {
	// ChatOpenAI + base_url → stream_usage 默认开 + chunk_timeout=240。
	cfg := &AppConfig{Models: []ModelConfig{{
		Name:     "gw",
		Use:      UseChatOpenAI,
		Settings: map[string]any{"base_url": "http://localhost:8000/v1"},
	}}}
	m, err := CreateChatModel("gw", false, cfg, nil)
	if err != nil {
		t.Fatalf("openai: %v", err)
	}
	if m.Settings["stream_usage"] != true {
		t.Errorf("stream_usage = %v, want true", m.Settings["stream_usage"])
	}
	if m.Settings["stream_chunk_timeout"] != 240.0 {
		t.Errorf("stream_chunk_timeout = %v, want 240", m.Settings["stream_chunk_timeout"])
	}

	// 非 OpenAI(Anthropic)→ stream_chunk_timeout 被剔除(factory.py:74-76)。
	cfg2 := &AppConfig{Models: []ModelConfig{{
		Name:     "claude",
		Use:      UseChatAnthropic,
		Settings: map[string]any{"stream_chunk_timeout": 123.0},
	}}}
	m2, err := CreateChatModel("claude", false, cfg2, nil)
	if err != nil {
		t.Fatalf("anthropic: %v", err)
	}
	if _, ok := m2.Settings["stream_chunk_timeout"]; ok {
		t.Errorf("stream_chunk_timeout should be dropped for non-OpenAI, got %v", m2.Settings["stream_chunk_timeout"])
	}
}

func TestDeepMergeDicts(t *testing.T) {
	base := map[string]any{"extra_body": map[string]any{"thinking": map[string]any{"type": "enabled"}, "keep": 1}}
	override := map[string]any{"extra_body": map[string]any{"thinking": map[string]any{"budget": 100}}}
	merged := deepMergeDicts(base, override)
	if got := nget(merged, "extra_body", "thinking", "type"); got != "enabled" {
		t.Errorf("deep merge lost nested key: %v", got)
	}
	if got := nget(merged, "extra_body", "thinking", "budget"); got != 100 {
		t.Errorf("deep merge lost override: %v", got)
	}
	if got := nget(merged, "extra_body", "keep"); got != 1 {
		t.Errorf("deep merge lost sibling: %v", got)
	}
	// 不改变入参。
	if _, ok := base["extra_body"].(map[string]any)["thinking"].(map[string]any)["budget"]; ok {
		t.Error("deepMergeDicts mutated input")
	}
}

// provider 补丁统一模式:reasoning_content 保留。
func TestRestoreReasoningContent(t *testing.T) {
	originals := []ChatMessage{
		{Role: "assistant", Content: "hello", Additional: map[string]any{"reasoning_content": "thinking..."}},
		{Role: "assistant", Content: "world"},
	}
	basePayload := PayloadMessage{
		"messages": []PayloadMessage{
			{"role": "assistant", "content": "hello"},
			{"role": "assistant", "content": "world"},
		},
	}
	out := DeepSeekBuildPayload(basePayload, originals)
	msgs := out["messages"].([]PayloadMessage)
	if msgs[0]["reasoning_content"] != "thinking..." {
		t.Errorf("reasoning_content not restored: %#v", msgs[0])
	}
	if _, ok := msgs[1]["reasoning_content"]; ok {
		t.Error("reasoning_content should not appear on messages without it")
	}
}

// 长度不一致时按签名 + ordinal 回退匹配(assistant_payload_replay.py)。
func TestRestoreAssistantPayloadsSignatureMatch(t *testing.T) {
	originals := []ChatMessage{
		{Role: "system", Content: "sys"},
		{Role: "assistant", Content: "A", ToolCalls: []map[string]any{{"id": "call_1", "name": "f", "args": "{}"}}},
		{Role: "tool", ToolCallID: "call_1", Content: "res"},
		{Role: "assistant", Content: "B"},
	}
	// 序列化时 system/tool 被合并/丢弃,只剩 assistant payload。
	payload := []PayloadMessage{
		{"role": "assistant", "content": "A", "tool_calls": []any{map[string]any{"id": "call_1", "name": "f", "arguments": "{}"}}},
		{"role": "assistant", "content": "B"},
	}
	// 给两条 assistant 原始消息分别打上不同 reasoning。
	originals[1].Additional = map[string]any{"reasoning_content": "r-A"}
	originals[3].Additional = map[string]any{"reasoning_content": "r-B"}

	RestoreAssistantPayloads(payload, originals, RestoreReasoningContent)
	if payload[0]["reasoning_content"] != "r-A" {
		t.Errorf("payload[0] reasoning = %v", payload[0]["reasoning_content"])
	}
	if payload[1]["reasoning_content"] != "r-B" {
		t.Errorf("payload[1] reasoning = %v", payload[1]["reasoning_content"])
	}
}

// vLLM:chat_template_kwargs 归一化 + reasoning 字段保留。
func TestVllmBuildPayload(t *testing.T) {
	base := PayloadMessage{
		"extra_body": map[string]any{
			"chat_template_kwargs": map[string]any{"thinking": true, "other": "keep"},
		},
		"messages": []PayloadMessage{
			{"role": "assistant", "content": "hi"},
		},
	}
	originals := []ChatMessage{
		{Role: "assistant", Content: "hi", Additional: map[string]any{"reasoning": "step by step"}},
	}
	out := VllmBuildPayload(base, originals)
	ctk := nget(out, "extra_body", "chat_template_kwargs").(map[string]any)
	if ctk["enable_thinking"] != true {
		t.Errorf("enable_thinking = %v, want true", ctk["enable_thinking"])
	}
	if _, ok := ctk["thinking"]; ok {
		t.Error("legacy 'thinking' key should be normalized away")
	}
	msgs := out["messages"].([]PayloadMessage)
	if msgs[0]["reasoning"] != "step by step" {
		t.Errorf("reasoning not restored: %#v", msgs[0])
	}
}

func TestReasoningToText(t *testing.T) {
	cases := []struct {
		in   any
		want string
	}{
		{"plain", "plain"},
		{[]any{"a", "b"}, "ab"},
		{map[string]any{"text": "hi"}, "hi"},
		{map[string]any{"content": "there"}, "there"},
		{map[string]any{"reasoning": "why"}, "why"},
		{map[string]any{"text": map[string]any{"content": "nested"}}, "nested"},
		{5, "5"},
	}
	for _, c := range cases {
		if got := ReasoningToText(c.in); got != c.want {
			t.Errorf("ReasoningToText(%v) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestCreateChatModelUnknownProvider(t *testing.T) {
	cfg := &AppConfig{Models: []ModelConfig{{Name: "x", Use: "unknown.module:UnknownModel"}}}
	if _, err := CreateChatModel("x", false, cfg, nil); err == nil {
		t.Error("expected error for unknown provider (resolve_class failed)")
	}
}

func TestCreateChatModelNameResolution(t *testing.T) {
	cfg := &AppConfig{Models: []ModelConfig{
		{Name: "first", Use: UseChatAnthropic},
		{Name: "second", Use: UseChatOpenAI},
	}}
	// name 空 → 第一个模型。
	m, err := CreateChatModel("", false, cfg, nil)
	if err != nil {
		t.Fatalf("default name: %v", err)
	}
	if m.Name != "first" || m.Dialect != "anthropic" {
		t.Errorf("default model = %s (%s)", m.Name, m.Dialect)
	}
	// 未找到 → 报错。
	if _, err := CreateChatModel("nope", false, cfg, nil); err == nil {
		t.Error("expected not-found error")
	}
}
