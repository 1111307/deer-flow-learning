package skills

import (
	"errors"
	"strings"
	"testing"
)

func TestParseSkillBasic(t *testing.T) {
	content := `---
name: pdf-editor
description: Edit PDF files
license: MIT
allowed-tools: [bash, read_file]
---
# PDF Editor
`
	s, err := ParseSkill(content, string(CategoryCustom), "pdf-editor", "/skills/custom/pdf-editor/SKILL.md")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s.Name != "pdf-editor" {
		t.Errorf("name = %q, want pdf-editor", s.Name)
	}
	if s.Description != "Edit PDF files" {
		t.Errorf("description = %q", s.Description)
	}
	if s.License != "MIT" {
		t.Errorf("license = %q, want MIT", s.License)
	}
	if len(s.AllowedTools) != 2 || s.AllowedTools[0] != "bash" || s.AllowedTools[1] != "read_file" {
		t.Errorf("allowed-tools = %v", s.AllowedTools)
	}
	if !s.Enabled {
		t.Errorf("parser must default enabled=true (parser.py:136)")
	}
	if s.Category != CategoryCustom {
		t.Errorf("category = %q", s.Category)
	}
}

// 三态 allowed-tools:省略=nil、[]=空非nil、[a,b]=白名单。
func TestParseSkillAllowedToolsThreeState(t *testing.T) {
	cases := []struct {
		name string
		fm   string
		want func([]string) bool
	}{
		{"omitted", "", func(v []string) bool { return v == nil }},
		{"empty", "allowed-tools: []\n", func(v []string) bool { return v != nil && len(v) == 0 }},
		{"null", "allowed-tools:\n", func(v []string) bool { return v == nil }},
		{"list", "allowed-tools: [a, b]\n", func(v []string) bool { return len(v) == 2 && v[0] == "a" && v[1] == "b" }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			content := "---\nname: n\ndescription: d\n" + c.fm + "---\n"
			s, err := ParseSkill(content, string(CategoryPublic), "x", "x/SKILL.md")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !c.want(s.AllowedTools) {
				t.Errorf("allowed-tools = %#v (nil=%v)", s.AllowedTools, s.AllowedTools == nil)
			}
		})
	}
}

func TestParseSkillErrors(t *testing.T) {
	if _, err := ParseSkill("no frontmatter", string(CategoryPublic), "x", "x/SKILL.md"); !errors.Is(err, ErrMissingFrontmatter) {
		t.Errorf("expected ErrMissingFrontmatter, got %v", err)
	}
	// frontmatter 是标量(非 mapping)→ ErrNotMapping。
	if _, err := ParseSkill("---\njust a scalar\n---\n", string(CategoryPublic), "x", "x/SKILL.md"); !errors.Is(err, ErrNotMapping) {
		t.Errorf("expected ErrNotMapping, got %v", err)
	}
	// 缺 name。
	if _, err := ParseSkill("---\ndescription: d\n---\n", string(CategoryPublic), "x", "x/SKILL.md"); err == nil || !strings.Contains(err.Error(), "name") {
		t.Errorf("expected missing name error, got %v", err)
	}
	// 缺 description。
	if _, err := ParseSkill("---\nname: n\n---\n", string(CategoryPublic), "x", "x/SKILL.md"); err == nil || !strings.Contains(err.Error(), "description") {
		t.Errorf("expected missing description error, got %v", err)
	}
}

// 非法 allowed-tools:非列表、含非字符串、含空名。
func TestParseAllowedToolsInvalid(t *testing.T) {
	bad := []string{
		"allowed-tools: bash\n",        // 标量
		"allowed-tools: [a, 1]\n",      // 含非字符串
		"allowed-tools: [a, \"  \"]\n", // 含空名
	}
	for _, fm := range bad {
		content := "---\nname: n\ndescription: d\n" + fm + "---\n"
		if _, err := ParseSkill(content, string(CategoryPublic), "x", "x/SKILL.md"); err == nil {
			t.Errorf("expected error for %q", fm)
		}
	}
}

// YAML 错误的行号对齐 + hint(parser.py::_format_yaml_error)。
func TestFormatYAMLErrorLineNumber(t *testing.T) {
	content := "---\nname: foo\ndescription: a: b\n---\n"
	_, err := ParseSkill(content, string(CategoryPublic), "x", "x/SKILL.md")
	if err == nil {
		t.Fatal("expected YAML error")
	}
	msg := err.Error()
	if !strings.Contains(msg, "Invalid YAML front-matter") {
		t.Errorf("missing header: %q", msg)
	}
	// 错误在 body 第 2 行(1-based)→ 文件第 3 行。
	if !strings.Contains(msg, "line 3:") {
		t.Errorf("expected 'line 3:' (file line alignment), got %q", msg)
	}
	// "mapping values are not allowed" + 冒号值 → hint。
	if !strings.Contains(msg, `description: "a: b"`) {
		t.Errorf("expected quoting hint, got %q", msg)
	}
}

func TestExtractJSONObject(t *testing.T) {
	cases := []struct {
		name  string
		raw   string
		ok    bool
		check func(m map[string]any) bool
	}{
		{"plain", `{"decision":"allow","reason":"ok"}`, true, func(m map[string]any) bool { return m["decision"] == "allow" }},
		{"fenced", "```json\n{\"decision\":\"warn\"}\n```", true, func(m map[string]any) bool { return m["decision"] == "warn" }},
		{"surrounded", `here is the result: {"decision":"block","reason":"x"} thanks`, true, func(m map[string]any) bool { return m["decision"] == "block" }},
		{"braces-in-string", `{"reason":"a { b } c","decision":"allow"}`, true, func(m map[string]any) bool { return m["reason"] == "a { b } c" }},
		{"not-json", "no json here", false, nil},
		{"array", `[1,2,3]`, false, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			m, ok := ExtractJSONObject(c.raw)
			if ok != c.ok {
				t.Fatalf("ok = %v, want %v", ok, c.ok)
			}
			if ok && !c.check(m) {
				t.Errorf("unexpected map: %#v", m)
			}
		})
	}
}

func TestScanSkillContent(t *testing.T) {
	t.Run("allow", func(t *testing.T) {
		model := func(_, _ string) (string, error) { return `{"decision":"allow","reason":"fine"}`, nil }
		res := ScanSkillContent("x", false, "", model)
		if res.Decision != "allow" || res.Reason != "fine" {
			t.Errorf("got %+v", res)
		}
	})
	t.Run("unparseable", func(t *testing.T) {
		model := func(_, _ string) (string, error) { return "not json", nil }
		res := ScanSkillContent("x", false, "", model)
		if res.Decision != "block" || !strings.Contains(res.Reason, "unparseable") {
			t.Errorf("got %+v", res)
		}
	})
	t.Run("model-error-non-executable", func(t *testing.T) {
		model := func(_, _ string) (string, error) { return "", errors.New("boom") }
		res := ScanSkillContent("x", false, "", model)
		if res.Decision != "block" || !strings.Contains(res.Reason, "unavailable for skill content") {
			t.Errorf("got %+v", res)
		}
	})
	t.Run("model-error-executable", func(t *testing.T) {
		model := func(_, _ string) (string, error) { return "", errors.New("boom") }
		res := ScanSkillContent("x", true, "", model)
		if res.Decision != "block" || !strings.Contains(res.Reason, "unavailable for executable content") {
			t.Errorf("got %+v", res)
		}
	})
	t.Run("nil-model", func(t *testing.T) {
		res := ScanSkillContent("x", false, "", nil)
		if res.Decision != "block" {
			t.Errorf("nil model must fail closed, got %+v", res)
		}
	})
	// decision 大小写归一化 + 缺 reason 默认。
	t.Run("normalize", func(t *testing.T) {
		model := func(_, _ string) (string, error) { return `{"decision":"BLOCK"}`, nil }
		res := ScanSkillContent("x", false, "", model)
		if res.Decision != "block" || res.Reason != "No reason provided." {
			t.Errorf("got %+v", res)
		}
	})
}

func TestSlashSkill(t *testing.T) {
	ref := ParseSlashSkillReference("/pdf-edit fix page 3")
	if ref == nil || ref.Name != "pdf-edit" || ref.RemainingText != "fix page 3" {
		t.Fatalf("got %+v", ref)
	}
	// 保留命令被排除。
	if ParseSlashSkillReference("/help me") != nil {
		t.Error("reserved command should be excluded")
	}
	// 非法语法。
	if ParseSlashSkillReference("no slash") != nil {
		t.Error("expected nil for non-slash text")
	}
	// 无剩余文本。
	if ref := ParseSlashSkillReference("/pdf-edit"); ref == nil || ref.RemainingText != "" {
		t.Errorf("expected empty remaining, got %+v", ref)
	}

	skills := []*Skill{
		{Name: "pdf-edit", Enabled: true, Category: CategoryCustom, RelativePath: "pdf-edit"},
		{Name: "other", Enabled: true, Category: CategoryCustom, RelativePath: "other"},
		{Name: "disabled", Enabled: false, Category: CategoryCustom, RelativePath: "disabled"},
	}
	res := ResolveSlashSkill("/pdf-edit do it", skills, nil, "/mnt/skills")
	if res == nil || res.Skill.Name != "pdf-edit" || res.RemainingText != "do it" {
		t.Fatalf("got %+v", res)
	}
	if res.ContainerFilePath != "/mnt/skills/custom/pdf-edit/SKILL.md" {
		t.Errorf("container path = %q", res.ContainerFilePath)
	}
	// availableSkills 白名单未命中 → 不激活。
	if ResolveSlashSkill("/pdf-edit do it", skills, map[string]bool{"other": true}, "/mnt/skills") != nil {
		t.Error("expected nil when not in available skills")
	}
	// disabled 不激活。
	if ResolveSlashSkill("/disabled do it", skills, nil, "/mnt/skills") != nil {
		t.Error("expected nil for disabled skill")
	}
}

func TestToolPolicy(t *testing.T) {
	type named struct{ n string }
	// named 必须实现 Name() string 才能满足 FilterToolsBySkillAllowedTools 的
	// 泛型约束 [T interface{ Name() string }]。Go 不允许在函数内定义方法,
	// 所以用包级类型 policyNamed(见文件底部)。
	skills := []*Skill{
		{Name: "a", AllowedTools: nil},           // 未声明
		{Name: "b", AllowedTools: []string{"x"}}, // 显式 [x]
		{Name: "c", AllowedTools: []string{"y", "z"}},
	}

	// 只要有显式声明,未声明者不再放行全部。
	allowed := AllowedToolNamesForSkills(skills)
	if allowed == nil || len(allowed) != 3 || !allowed["x"] || !allowed["y"] || !allowed["z"] {
		t.Errorf("allowed = %v", allowed)
	}

	// 全部未声明 → nil(legacy allow-all)。
	if AllowedToolNamesForSkills([]*Skill{{Name: "a"}}) != nil {
		t.Error("expected nil for no explicit declaration")
	}
	// 空列表。
	if AllowedToolNamesForSkills(nil) != nil {
		t.Error("expected nil for no skills")
	}

	// 过滤。
	tools := []policyNamed{{"x"}, {"y"}, {"w"}}
	filtered := FilterToolsBySkillAllowedTools(tools, skills)
	if len(filtered) != 2 || filtered[0].n != "x" || filtered[1].n != "y" {
		t.Errorf("filtered = %v", filtered)
	}

	// 全部未声明 → 原样返回。
	if got := FilterToolsBySkillAllowedTools(tools, []*Skill{{Name: "a"}}); len(got) != 3 {
		t.Errorf("expected unfiltered, got %v", got)
	}
}

func TestSkillContainerPath(t *testing.T) {
	s := &Skill{Name: "x", Category: CategoryPublic, RelativePath: "sub/dir"}
	if p := s.GetContainerPath("/mnt/skills"); p != "/mnt/skills/public/sub/dir" {
		t.Errorf("path = %q", p)
	}
	// relative "." → 空 skill_path。
	s2 := &Skill{Name: "x", Category: CategoryPublic, RelativePath: "."}
	if p := s2.GetContainerFilePath("/mnt/skills"); p != "/mnt/skills/public/SKILL.md" {
		t.Errorf("path = %q", p)
	}
}

// policyNamed 是 TestToolPolicy 用的测试工具类型。
// Go 不允许在函数内定义方法,所以提到包级;实现 Name() string 以满足
// FilterToolsBySkillAllowedTools 的泛型约束 [T interface{ Name() string }]。
type policyNamed struct{ n string }

func (p policyNamed) Name() string { return p.n }
