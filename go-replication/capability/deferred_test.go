package capability

import (
	"context"
	"strings"
	"testing"
)

// fakeMCPTool 是测试用 MCP 工具(带 deerflow_mcp 标记)。
type fakeMCPTool struct{ name, desc string }

func (f *fakeMCPTool) Name() string                                    { return f.name }
func (f *fakeMCPTool) Description() string                             { return f.desc }
func (f *fakeMCPTool) Run(_ context.Context, _ string) (string, error) { return "ok", nil }
func (f *fakeMCPTool) IsMCPTool() bool                                 { return true }

// fakePlainTool 是测试用普通工具(不带 MCP 标记)。
type fakePlainTool struct{ name, desc string }

func (f *fakePlainTool) Name() string                                    { return f.name }
func (f *fakePlainTool) Description() string                             { return f.desc }
func (f *fakePlainTool) Run(_ context.Context, _ string) (string, error) { return "ok", nil }

func sampleCatalog() *DeferredCatalog {
	return NewDeferredCatalog([]Tool{
		&fakeMCPTool{name: "slack_post_message", desc: "send a message to a Slack channel"},
		&fakeMCPTool{name: "slack_search", desc: "search Slack history"},
		&fakeMCPTool{name: "github_create_issue", desc: "create a GitHub issue"},
		&fakeMCPTool{name: "github_list_repos", desc: "list GitHub repositories"},
		&fakeMCPTool{name: "notebook_execute", desc: "run a jupyter notebook cell"},
		&fakeMCPTool{name: "notebook_read", desc: "read a jupyter notebook"},
	})
}

func TestCatalogNamesSortedAndHashDeterministic(t *testing.T) {
	c := sampleCatalog()
	names := c.Names()
	if len(names) != 6 {
		t.Fatalf("expected 6 names, got %d", len(names))
	}
	for i := 1; i < len(names); i++ {
		if names[i] < names[i-1] {
			t.Fatalf("names not sorted: %v", names)
		}
	}
	if c.Hash() != c.Hash() {
		t.Fatalf("hash not deterministic")
	}
	if len(c.Hash()) != 16 {
		t.Fatalf("hash should be 16 hex chars, got %q", c.Hash())
	}
}

func TestCatalogHashChangesOnDrift(t *testing.T) {
	a := NewDeferredCatalog([]Tool{&fakeMCPTool{name: "x", desc: "old"}})
	b := NewDeferredCatalog([]Tool{&fakeMCPTool{name: "x", desc: "new schema"}})
	if a.Hash() == b.Hash() {
		t.Fatalf("catalog drift should change hash")
	}
}

func TestSearchSelect(t *testing.T) {
	c := sampleCatalog()
	hits := c.Search("select:github_create_issue, slack_search")
	if len(hits) != 2 {
		t.Fatalf("select should return 2, got %d", len(hits))
	}
	got := map[string]bool{}
	for _, h := range hits {
		got[h.Name()] = true
	}
	if !got["github_create_issue"] || !got["slack_search"] {
		t.Fatalf("select returned wrong tools: %v", got)
	}
}

func TestSearchPlusToken(t *testing.T) {
	c := sampleCatalog()
	hits := c.Search("+slack")
	if len(hits) != 2 {
		t.Fatalf("+slack should return 2 slack tools, got %d", len(hits))
	}
	for _, h := range hits {
		if !strings.Contains(strings.ToLower(h.Name()), "slack") {
			t.Fatalf("+slack returned non-slack tool %q", h.Name())
		}
	}
	// bare "+" should return nothing
	if got := c.Search("+"); got != nil {
		t.Fatalf("bare + should return nil, got %v", got)
	}
}

func TestSearchRegexAndLimit(t *testing.T) {
	c := sampleCatalog()
	// regex/keyword search, name match scores higher
	hits := c.Search("github")
	if len(hits) != 2 {
		t.Fatalf("github search should return 2, got %d", len(hits))
	}
	// name match (github_*) should come before any description-only match
	if hits[0].Name() == "" || !strings.Contains(hits[0].Name(), "github") {
		t.Fatalf("name matches should be ranked first")
	}

	// empty query returns nil
	if got := c.Search("   "); got != nil {
		t.Fatalf("empty query should return nil")
	}

	// invalid regex degrades to literal match (no panic),对应 _compile_catalog_regex。
	re := compileCatalogRegex("(unbalanced")
	if !re.MatchString("this has (unbalanced text") {
		t.Fatalf("invalid regex should degrade to literal match")
	}
}

func TestSearchLimitMaxResults(t *testing.T) {
	tools := make([]Tool, 0, 10)
	for i := 0; i < 10; i++ {
		tools = append(tools, &fakeMCPTool{name: "common_tool", desc: "description"})
	}
	c := NewDeferredCatalog(tools)
	if got := c.Search("common"); len(got) != MaxDeferredResults {
		t.Fatalf("search should be limited to %d, got %d", MaxDeferredResults, len(got))
	}
}

func TestToolSearchPromotes(t *testing.T) {
	c := sampleCatalog()
	tool := buildToolSearchTool(c)

	out, err := tool.Run(context.Background(), `{"query":"select:slack_post_message"}`)
	if err != nil {
		t.Fatalf("Run error: %v", err)
	}
	if !strings.Contains(out, "slack_post_message") {
		t.Fatalf("tool_search output should contain schema, got %q", out)
	}

	promoter, ok := tool.(Promoter)
	if !ok {
		t.Fatalf("tool_search tool should implement Promoter")
	}
	p := promoter.Promoted()
	if p == nil {
		t.Fatalf("Promoted() should not be nil after Run")
	}
	if p.CatalogHash != c.Hash() {
		t.Fatalf("promoted hash mismatch: %q != %q", p.CatalogHash, c.Hash())
	}
	if len(p.Names) != 1 || p.Names[0] != "slack_post_message" {
		t.Fatalf("promoted names wrong: %v", p.Names)
	}
}

func TestToolSearchNoMatch(t *testing.T) {
	c := sampleCatalog()
	tool := buildToolSearchTool(c)
	out, _ := tool.Run(context.Background(), `{"query":"select:does_not_exist"}`)
	if !strings.Contains(out, "No tools found") {
		t.Fatalf("expected no-match message, got %q", out)
	}
	if p := tool.(Promoter).Promoted(); len(p.Names) != 0 {
		t.Fatalf("no-match should promote empty names, got %v", p.Names)
	}
}

func TestBuildDeferredToolSetup(t *testing.T) {
	mcp := []Tool{
		&fakeMCPTool{name: "slack_post_message", desc: "d"},
		&fakePlainTool{name: "read_file", desc: "d"},
	}

	// disabled -> empty
	if s := BuildDeferredToolSetup(mcp, false); !s.Empty() {
		t.Fatalf("disabled should produce empty setup")
	}

	// enabled but no MCP -> empty
	if s := BuildDeferredToolSetup([]Tool{&fakePlainTool{name: "read_file", desc: "d"}}, true); !s.Empty() {
		t.Fatalf("enabled without MCP should produce empty setup")
	}

	// enabled with MCP -> populated
	s := BuildDeferredToolSetup(mcp, true)
	if s.Empty() {
		t.Fatalf("enabled with MCP should produce populated setup")
	}
	if s.ToolSearchTool == nil || s.CatalogHash == "" || len(s.DeferredNames) != 1 {
		t.Fatalf("setup fields wrong: %+v", s)
	}
	if s.DeferredNames[0] != "slack_post_message" {
		t.Fatalf("deferred names should only include MCP tools, got %v", s.DeferredNames)
	}
}

func TestAssembleDeferredToolsAppendsToolSearch(t *testing.T) {
	mcp := []Tool{&fakeMCPTool{name: "slack_post_message", desc: "d"}}
	final, setup, err := AssembleDeferredTools(mcp, true)
	if err != nil {
		t.Fatalf("AssembleDeferredTools error: %v", err)
	}
	if len(final) != 2 {
		t.Fatalf("should append tool_search, got %d tools", len(final))
	}
	if final[len(final)-1].Name() != "tool_search" {
		t.Fatalf("tool_search should be appended last, got %q", final[len(final)-1].Name())
	}
	if setup.Empty() {
		t.Fatalf("setup should not be empty")
	}
}

func TestPromptSection(t *testing.T) {
	if got := GetDeferredToolsPromptSection(nil); got != "" {
		t.Fatalf("empty names should render empty, got %q", got)
	}
	got := GetDeferredToolsPromptSection([]string{"b", "a"})
	if !strings.Contains(got, "a") || !strings.Contains(got, "b") {
		t.Fatalf("prompt section missing names: %q", got)
	}
	if !strings.Contains(got, "<available-deferred-tools>") {
		t.Fatalf("prompt section missing tag: %q", got)
	}
	// sorted: "a" before "b"
	if strings.Index(got, "a") > strings.Index(got, "b") {
		t.Fatalf("names should be sorted: %q", got)
	}
}

func TestDeferredToolFilterMiddleware(t *testing.T) {
	deferred := []string{"slack_post_message", "github_create_issue"}
	m := NewDeferredToolFilterMiddleware(deferred, "hash123")

	allTools := []Tool{
		&fakeMCPTool{name: "slack_post_message", desc: "d"},
		&fakeMCPTool{name: "github_create_issue", desc: "d"},
		&fakePlainTool{name: "read_file", desc: "d"},
	}

	// 无提升:两个 deferred 都被隐藏,只留 read_file。
	filtered := m.FilterTools(allTools, nil)
	if len(filtered) != 1 || filtered[0].Name() != "read_file" {
		t.Fatalf("should hide all deferred, got %v", namesOf(filtered))
	}

	// 阻止未提升的 deferred 工具调用。
	if msg, blocked := m.BlockedToolMessage("slack_post_message", nil); !blocked || !strings.Contains(msg, "tool_search") {
		t.Fatalf("should block unpromoted tool, got blocked=%v msg=%q", blocked, msg)
	}
	if _, blocked := m.BlockedToolMessage("read_file", nil); blocked {
		t.Fatalf("should not block non-deferred tool")
	}

	// 提升 slack_post_message(哈希匹配)→ slack 可见,github 仍隐藏。
	promoted := &Promoted{CatalogHash: "hash123", Names: []string{"slack_post_message"}}
	filtered = m.FilterTools(allTools, promoted)
	if len(filtered) != 2 {
		t.Fatalf("after promoting slack, should be 2 tools, got %d", len(filtered))
	}
	if _, blocked := m.BlockedToolMessage("slack_post_message", promoted); blocked {
		t.Fatalf("promoted tool should not be blocked")
	}
	if _, blocked := m.BlockedToolMessage("github_create_issue", promoted); !blocked {
		t.Fatalf("unpromoted github tool should still be blocked")
	}
}

func TestDeferredToolFilterCatalogHashPartition(t *testing.T) {
	m := NewDeferredToolFilterMiddleware([]string{"slack_post_message"}, "hashA")
	// 哈希不匹配 → 旧提升名单失效,slack 仍被隐藏。
	stale := &Promoted{CatalogHash: "hashB", Names: []string{"slack_post_message"}}
	filtered := m.FilterTools([]Tool{&fakeMCPTool{name: "slack_post_message", desc: "d"}}, stale)
	if len(filtered) != 0 {
		t.Fatalf("stale promotion (hash mismatch) should not expose tool, got %d", len(filtered))
	}
}

func namesOf(tools []Tool) []string {
	out := make([]string, 0, len(tools))
	for _, t := range tools {
		out = append(out, t.Name())
	}
	return out
}
