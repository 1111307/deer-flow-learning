package capability

import (
	"context"
	"testing"
)

// fakeSandbox / fakeProvider 是测试用最小实现。
type fakeSandbox struct{ id string }

func (f *fakeSandbox) ID() string                           { return f.id }
func (f *fakeSandbox) ExecuteCommand(string) string         { return "" }
func (f *fakeSandbox) ReadFile(string) (string, error)      { return "", nil }
func (f *fakeSandbox) DownloadFile(string) ([]byte, error)  { return nil, nil }
func (f *fakeSandbox) ListDir(string, int) []string         { return nil }
func (f *fakeSandbox) WriteFile(string, string, bool) error { return nil }
func (f *fakeSandbox) Glob(string, string, bool, int) ([]string, bool, error) {
	return nil, false, nil
}
func (f *fakeSandbox) Grep(string, string, string, bool, bool, int) ([]GrepMatch, bool, error) {
	return nil, false, nil
}
func (f *fakeSandbox) UpdateFile(string, []byte) error { return nil }

type fakeProvider struct{ created int }

func (p *fakeProvider) Acquire(string) (string, error) { return "fake:1", nil }
func (p *fakeProvider) Get(string) (Sandbox, bool)     { return &fakeSandbox{id: "fake:1"}, true }
func (p *fakeProvider) Release(string)                 {}
func (p *fakeProvider) Reset()                         {}

func TestRegisterAndNewSandboxProvider(t *testing.T) {
	RegisterSandboxProvider("test-provider", func() SandboxProvider { return &fakeProvider{} })

	p, err := NewSandboxProvider("test-provider")
	if err != nil {
		t.Fatalf("expected provider, got error: %v", err)
	}
	if _, ok := p.(*fakeProvider); !ok {
		t.Fatalf("expected *fakeProvider, got %T", p)
	}

	if _, err := NewSandboxProvider("does-not-exist"); err == nil {
		t.Fatal("expected error for unknown provider")
	}
}

func TestGetSandboxProviderSingleton(t *testing.T) {
	RegisterSandboxProvider("singleton-test", func() SandboxProvider { return &fakeProvider{} })
	SetSandboxProviderName("singleton-test")
	defer SetSandboxProviderName("local")

	p1 := GetSandboxProvider()
	p2 := GetSandboxProvider()
	if p1 != p2 {
		t.Fatal("GetSandboxProvider should return the same singleton instance")
	}

	// 注入自定义 provider 后应返回注入实例。
	custom := &fakeProvider{}
	SetSandboxProvider(custom)
	if got := GetSandboxProvider(); got != custom {
		t.Fatal("SetSandboxProvider should override the singleton")
	}
}

func TestResetSandboxProvider(t *testing.T) {
	RegisterSandboxProvider("reset-test", func() SandboxProvider { return &fakeProvider{} })
	SetSandboxProviderName("reset-test")
	defer SetSandboxProviderName("local")

	p1 := GetSandboxProvider()
	ResetSandboxProvider()
	p2 := GetSandboxProvider()
	if p1 == p2 {
		t.Fatal("ResetSandboxProvider should clear the cached instance")
	}
}

func TestThreadIDContext(t *testing.T) {
	ctx := WithThreadID(context.Background(), "thread-123")
	if got := ThreadIDFrom(ctx); got != "thread-123" {
		t.Fatalf("expected thread-123, got %q", got)
	}
	if got := ThreadIDFrom(context.Background()); got != "" {
		t.Fatalf("expected empty thread id, got %q", got)
	}
}
