package provider

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"deerflow-go/capability"
)

func TestLocalSandboxWriteReadList(t *testing.T) {
	dir := t.TempDir()
	sb := NewLocalSandbox("local", []PathMapping{
		{ContainerPath: VirtualPathPrefix, LocalPath: dir},
	})

	if err := sb.WriteFile(VirtualPathPrefix+"/workspace/hello.txt", "hello world", false); err != nil {
		t.Fatalf("write: %v", err)
	}
	content, err := sb.ReadFile(VirtualPathPrefix + "/workspace/hello.txt")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if content != "hello world" {
		t.Fatalf("expected 'hello world', got %q", content)
	}

	// 目录列表应包含 workspace/(带尾 /)。
	entries := sb.ListDir(VirtualPathPrefix, 2)
	found := false
	for _, e := range entries {
		if strings.Contains(e, "workspace") && strings.HasSuffix(e, "/") {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected workspace/ in listing, got %v", entries)
	}
}

func TestLocalSandboxPathTraversal(t *testing.T) {
	dir := t.TempDir()
	sb := NewLocalSandbox("local", []PathMapping{
		{ContainerPath: VirtualPathPrefix, LocalPath: dir},
	})

	// 写入 .. 越界应被拒绝。
	if err := sb.WriteFile(VirtualPathPrefix+"/../escape.txt", "x", false); err == nil {
		t.Fatal("expected path traversal error")
	}
}

func TestLocalSandboxReadOnlyMount(t *testing.T) {
	dir := t.TempDir()
	sb := NewLocalSandbox("local", []PathMapping{
		{ContainerPath: "/mnt/skills", LocalPath: dir, ReadOnly: true},
	})

	if err := sb.WriteFile("/mnt/skills/x.txt", "x", false); err == nil {
		t.Fatal("expected read-only file system error")
	}
}

func TestLocalSandboxReverseResolveOutput(t *testing.T) {
	dir := t.TempDir()
	sb := NewLocalSandbox("local", []PathMapping{
		{ContainerPath: VirtualPathPrefix, LocalPath: dir},
	})

	// 内容里含容器路径,写盘后应解析成本地路径;读回时(agent 写过)反向掩码回容器路径。
	content := "see " + VirtualPathPrefix + "/workspace/a.txt"
	if err := sb.WriteFile(VirtualPathPrefix+"/workspace/note.txt", content, false); err != nil {
		t.Fatalf("write: %v", err)
	}
	got, err := sb.ReadFile(VirtualPathPrefix + "/workspace/note.txt")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !strings.Contains(got, VirtualPathPrefix+"/workspace/a.txt") {
		t.Fatalf("expected reverse-resolved virtual path in output, got %q", got)
	}
	// 本地宿主路径不应泄漏。
	if strings.Contains(got, filepath.ToSlash(dir)) && !strings.Contains(dir, "user-data") {
		t.Fatalf("host path leaked into output: %q", got)
	}
}

func TestLocalSandboxProviderLRU(t *testing.T) {
	p := NewLocalSandboxProvider(t.TempDir(), 2)

	id1, _ := p.Acquire("t1")
	id2, _ := p.Acquire("t2")
	// 触摸 t1,把它变成最近使用 → 应存活;t2 变最少使用 → 应被淘汰。
	if _, ok := p.Get(id1); !ok {
		t.Fatal("t1 should exist")
	}
	_ = id2

	// 第三个线程触发 LRU 淘汰 t2(最少使用)。
	p.Acquire("t3")
	if _, ok := p.Get(id2); ok {
		t.Fatal("t2 should have been evicted (LRU cap=2)")
	}
	if _, ok := p.Get(id1); !ok {
		t.Fatal("t1 should survive (most-recently-used)")
	}
}

func TestDeterministicSandboxID(t *testing.T) {
	a := deterministicSandboxID("thread-abc")
	b := deterministicSandboxID("thread-abc")
	if a != b {
		t.Fatalf("deterministic id should be stable: %q != %q", a, b)
	}
	if deterministicSandboxID("thread-xyz") == a {
		t.Fatal("different threads should yield different ids")
	}
}

// mockBackend 测试用后端,记录 create/destroy 调用。
type mockBackend struct {
	created   []string
	destroyed []string
	running   []sandboxInfo
}

func (m *mockBackend) create(name, image string) error {
	m.created = append(m.created, name)
	return nil
}
func (m *mockBackend) destroy(name string) error {
	m.destroyed = append(m.destroyed, name)
	return nil
}
func (m *mockBackend) listRunning(prefix string) ([]sandboxInfo, error) {
	return m.running, nil
}

func TestDockerProviderWarmPoolReclaim(t *testing.T) {
	be := &mockBackend{}
	p := NewDockerSandboxProvider("img", "pre", 3, be)

	id1, err := p.Acquire("thread-1")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if len(be.created) != 1 {
		t.Fatalf("expected 1 create, got %d", len(be.created))
	}

	// release → warm pool。
	p.Release(id1)
	if _, ok := p.Get(id1); ok {
		t.Fatal("released sandbox should not be active")
	}

	// 再次 acquire 同一 thread → 从 warm pool 回收,不重新 create。
	id2, err := p.Acquire("thread-1")
	if err != nil {
		t.Fatalf("re-acquire: %v", err)
	}
	if id2 != id1 {
		t.Fatalf("expected same id %q, got %q", id1, id2)
	}
	if len(be.created) != 1 {
		t.Fatalf("warm pool reclaim should not re-create; creates=%d", len(be.created))
	}
}

func TestDockerProviderReplicasSoftCap(t *testing.T) {
	be := &mockBackend{}
	p := NewDockerSandboxProvider("img", "pre", 2, be)

	// 两个活跃容器 + 一个 warm pool 容器,再 create 会挤出 warm。
	id1, _ := p.Acquire("a")
	id2, _ := p.Acquire("b")
	p.Release(id2) // b 进 warm pool

	// 现在 active=1(a), warm=1(b), total=2 >= replicas=2。
	// 创建 c 时应先挤出 warm 里的 b。
	id3, err := p.Acquire("c")
	if err != nil {
		t.Fatalf("acquire c: %v", err)
	}
	_ = id1
	_ = id3

	destroyed := map[string]bool{}
	for _, d := range be.destroyed {
		destroyed[d] = true
	}
	if !destroyed[p.containerName(id2)] {
		t.Fatalf("expected warm-pool sandbox %s to be evicted, destroyed=%v", id2, be.destroyed)
	}
}

func TestRejectPathTraversal(t *testing.T) {
	if err := rejectPathTraversal("/mnt/user-data/../etc/passwd"); err == nil {
		t.Fatal("expected traversal rejection")
	}
	if err := rejectPathTraversal("/mnt/user-data/workspace/a.txt"); err != nil {
		t.Fatalf("unexpected rejection: %v", err)
	}
	if err := rejectPathTraversal(`C:\..\etc`); err == nil {
		t.Fatal("expected windows-style traversal rejection")
	}
}

func TestValidateBashCommandPaths(t *testing.T) {
	if err := validateBashCommandPaths("curl file:///etc/passwd"); err == nil {
		t.Fatal("expected file:// rejection")
	}
	if err := validateBashCommandPaths("cat ../secret"); err == nil {
		t.Fatal("expected .. rejection")
	}
	if err := validateBashCommandPaths("ls /mnt/user-data/workspace"); err != nil {
		t.Fatalf("unexpected rejection: %v", err)
	}
}

func TestWriteFile80KBLimit(t *testing.T) {
	p := NewLocalSandboxProvider(t.TempDir(), 8)
	tool := &writeFileTool{provider: p}

	big := strings.Repeat("a", DefaultWriteFileMaxBytes+1)
	args := `{"path":"/mnt/user-data/workspace/big.txt","content":"` + big + `"}`
	out, err := tool.Run(capability.WithThreadID(context.Background(), "t"), args)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !strings.Contains(out, "exceeds") {
		t.Fatalf("expected 80KB limit error, got %q", out)
	}
}

func TestGlobMatch(t *testing.T) {
	cases := []struct {
		pattern, path string
		want          bool
	}{
		{"*.py", "a.py", true},
		{"*.py", "a/b.py", false},
		{"**/*.py", "a/b.py", true},
		{"**/*.py", "a.py", true},
		{"**/test_*.go", "x/y/test_a.go", true},
	}
	for _, c := range cases {
		if got := pathMatches(c.pattern, c.path); got != c.want {
			t.Errorf("pathMatches(%q, %q) = %v, want %v", c.pattern, c.path, got, c.want)
		}
	}
}

func TestShouldIgnoreName(t *testing.T) {
	for _, name := range []string{".git", "node_modules", "__pycache__", "*.log"} {
		if !shouldIgnoreName(name) {
			t.Errorf("expected %q to be ignored", name)
		}
	}
	if shouldIgnoreName("main.go") {
		t.Error("main.go should not be ignored")
	}
}

func TestFindGrepMatches(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte("hello world\nfoo bar\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	matches, truncated, err := findGrepMatches(dir, "hello", "", false, false, 100, DefaultMaxFileSizeBytes, DefaultLineSummaryLength)
	if err != nil {
		t.Fatalf("grep: %v", err)
	}
	if truncated || len(matches) != 1 {
		t.Fatalf("expected 1 match, got %d (truncated=%v)", len(matches), truncated)
	}
	if matches[0].LineNumber != 1 || !strings.Contains(matches[0].Line, "hello") {
		t.Fatalf("unexpected match: %+v", matches[0])
	}
}

func TestListDirTreeDepth(t *testing.T) {
	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, "a", "b"), 0o755)
	os.WriteFile(filepath.Join(dir, "a", "b", "c.txt"), []byte("x"), 0o644)
	os.WriteFile(filepath.Join(dir, "top.txt"), []byte("x"), 0o644)

	entries := listDirTree(dir, 2)
	var hasTop, hasAB, hasC bool
	for _, e := range entries {
		e = filepath.ToSlash(e)
		switch {
		case strings.HasSuffix(e, "top.txt"):
			hasTop = true
		case strings.Contains(e, "a/b/"):
			hasAB = true
		case strings.HasSuffix(e, "c.txt"):
			hasC = true
		}
	}
	if !hasTop || !hasAB {
		t.Fatalf("expected top.txt and a/b/, got %v", entries)
	}
	if hasC {
		t.Fatalf("c.txt is depth 3, should not appear at maxDepth 2: %v", entries)
	}
}
