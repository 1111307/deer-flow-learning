package mcp

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"deerflow-go/capability"
)

// ---------------------------------------------------------------------------
// BuildServerParams
// ---------------------------------------------------------------------------

func TestBuildServerParams(t *testing.T) {
	t.Run("stdio", func(t *testing.T) {
		p, err := BuildServerParams("srv", McpServerConfig{Command: "npx", Args: []string{"-y", "x"}})
		if err != nil {
			t.Fatal(err)
		}
		if p["transport"] != "stdio" || p["command"] != "npx" {
			t.Errorf("got %v", p)
		}
		if args, ok := p["args"].([]string); !ok || len(args) != 2 {
			t.Errorf("args = %v", p["args"])
		}
	})
	t.Run("stdio-missing-command", func(t *testing.T) {
		if _, err := BuildServerParams("srv", McpServerConfig{}); err == nil {
			t.Error("expected error for stdio without command")
		}
	})
	t.Run("sse", func(t *testing.T) {
		p, err := BuildServerParams("srv", McpServerConfig{Type: "sse", URL: "http://x/sse", Headers: map[string]string{"A": "b"}})
		if err != nil {
			t.Fatal(err)
		}
		if p["transport"] != "sse" || p["url"] != "http://x/sse" {
			t.Errorf("got %v", p)
		}
	})
	t.Run("http-missing-url", func(t *testing.T) {
		if _, err := BuildServerParams("srv", McpServerConfig{Type: "http"}); err == nil {
			t.Error("expected error for http without url")
		}
	})
	t.Run("unsupported", func(t *testing.T) {
		if _, err := BuildServerParams("srv", McpServerConfig{Type: "bogus"}); err == nil {
			t.Error("expected error for unsupported transport")
		}
	})
}

func TestBuildServersConfigSkipsBadServer(t *testing.T) {
	ext := ExtensionsConfig{MCPServers: map[string]McpServerConfig{
		"good": {Command: "npx"},
		"bad":  {Type: "sse"}, // missing url
	}}
	cfg := BuildServersConfig(ext)
	if _, ok := cfg["good"]; !ok {
		t.Errorf("good server missing: %v", cfg)
	}
	if _, ok := cfg["bad"]; ok {
		t.Errorf("bad server should be skipped: %v", cfg)
	}
}

// ---------------------------------------------------------------------------
// SessionPool
// ---------------------------------------------------------------------------

// countingFactory 记录 Open/Close 次数,可阻塞 Open 以模拟慢连接。
type countingFactory struct {
	mu      sync.Mutex
	opens   int
	closes  int
	blockCh chan struct{}
}

func (f *countingFactory) Open(ctx context.Context, _ map[string]any) (*Session, error) {
	f.mu.Lock()
	f.opens++
	n := f.opens
	ch := f.blockCh
	f.mu.Unlock()
	if ch != nil {
		select {
		case <-ch:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return &Session{ID: fmt.Sprintf("s-%d", n)}, nil
}

func (f *countingFactory) Close(*Session) {
	f.mu.Lock()
	f.closes++
	f.mu.Unlock()
}

func (f *countingFactory) counts() (opens, closes int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.opens, f.closes
}

func TestSessionPoolReuseAndIsolation(t *testing.T) {
	f := &countingFactory{}
	p := NewSessionPool(f)
	ctx := context.Background()

	s1a, err := p.GetSession(ctx, "server", "scope", nil)
	if err != nil {
		t.Fatal(err)
	}
	s1b, err := p.GetSession(ctx, "server", "scope", nil)
	if err != nil {
		t.Fatal(err)
	}
	if s1a != s1b {
		t.Error("same (server, scope) must reuse the same session")
	}
	// 不同 scope → 不同会话。
	s2, err := p.GetSession(ctx, "server", "other-scope", nil)
	if err != nil {
		t.Fatal(err)
	}
	if s2 == s1a {
		t.Error("different scope must be isolated")
	}
	opens, _ := f.counts()
	if opens != 2 {
		t.Errorf("opens = %d, want 2", opens)
	}
	if p.Len() != 2 {
		t.Errorf("Len = %d, want 2", p.Len())
	}
}

func TestSessionPoolLRUEviction(t *testing.T) {
	f := &countingFactory{}
	p := NewSessionPool(f)
	p.max = 2
	ctx := context.Background()

	p.GetSession(ctx, "s", "1", nil)
	p.GetSession(ctx, "s", "2", nil)
	// 第 3 个触发 LRU 驱逐最旧的 s1。
	p.GetSession(ctx, "s", "3", nil)

	_, closes := f.counts()
	if closes != 1 {
		t.Errorf("closes = %d, want 1 (evict oldest)", closes)
	}
	if p.Len() != 2 {
		t.Errorf("Len = %d, want 2", p.Len())
	}
}

func TestSessionPoolLRUTouch(t *testing.T) {
	f := &countingFactory{}
	p := NewSessionPool(f)
	p.max = 2
	ctx := context.Background()

	p.GetSession(ctx, "s", "1", nil)
	p.GetSession(ctx, "s", "2", nil)
	// touch s1(变成最近使用),再插入 s3 时应驱逐 s2 而非 s1。
	s1, _ := p.GetSession(ctx, "s", "1", nil)
	p.GetSession(ctx, "s", "3", nil)

	// s1 仍是活会话(未关闭)。
	opens, closes := f.counts()
	_ = s1
	if opens != 3 {
		t.Errorf("opens = %d, want 3", opens)
	}
	if closes != 1 {
		t.Errorf("closes = %d, want 1 (s2 evicted)", closes)
	}
}

func TestSessionPoolInflightDedup(t *testing.T) {
	f := &countingFactory{blockCh: make(chan struct{})}
	p := NewSessionPool(f)

	var wg sync.WaitGroup
	sessions := make([]*Session, 2)
	errs := make([]error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sessions[i], errs[i] = p.GetSession(context.Background(), "srv", "scope", nil)
		}(i)
	}
	// 给第一个 goroutine 时间进入 Open 并阻塞;第二个应 join 而非重复创建。
	time.Sleep(50 * time.Millisecond)
	close(f.blockCh)
	wg.Wait()

	opens, _ := f.counts()
	if opens != 1 {
		t.Errorf("opens = %d, want 1 (in-flight dedup)", opens)
	}
	for i := range errs {
		if errs[i] != nil {
			t.Errorf("errs[%d] = %v", i, errs[i])
		}
	}
	if sessions[0] != sessions[1] {
		t.Error("joiners must share the same session")
	}
}

func TestSessionPoolClose(t *testing.T) {
	t.Run("close-scope", func(t *testing.T) {
		f := &countingFactory{}
		p := NewSessionPool(f)
		ctx := context.Background()
		p.GetSession(ctx, "s", "scope-a", nil)
		p.GetSession(ctx, "s", "scope-b", nil)
		p.CloseScope("scope-a")
		_, closes := f.counts()
		if closes != 1 {
			t.Errorf("closes = %d, want 1", closes)
		}
		if p.Len() != 1 {
			t.Errorf("Len = %d, want 1", p.Len())
		}
	})
	t.Run("close-server", func(t *testing.T) {
		f := &countingFactory{}
		p := NewSessionPool(f)
		ctx := context.Background()
		p.GetSession(ctx, "srv-a", "scope", nil)
		p.GetSession(ctx, "srv-b", "scope", nil)
		p.CloseServer("srv-a")
		_, closes := f.counts()
		if closes != 1 {
			t.Errorf("closes = %d, want 1", closes)
		}
	})
	t.Run("close-all", func(t *testing.T) {
		f := &countingFactory{}
		p := NewSessionPool(f)
		ctx := context.Background()
		p.GetSession(ctx, "srv", "scope-1", nil)
		p.GetSession(ctx, "srv", "scope-2", nil)
		p.CloseAll()
		_, closes := f.counts()
		if closes != 2 {
			t.Errorf("closes = %d, want 2", closes)
		}
		if p.Len() != 0 {
			t.Errorf("Len = %d, want 0", p.Len())
		}
	})
}

// 创建过程中被 CloseAll 移除:in-flight owner 被取消,创建方拿到错误且不泄漏会话。
func TestSessionPoolClosedWhileCreating(t *testing.T) {
	f := &countingFactory{blockCh: make(chan struct{})}
	p := NewSessionPool(f)

	type result struct {
		s   *Session
		err error
	}
	done := make(chan result, 1)
	go func() {
		s, err := p.GetSession(context.Background(), "srv", "scope", nil)
		done <- result{s, err}
	}()

	time.Sleep(30 * time.Millisecond)
	p.CloseAll() // 创建还没完成,把 in-flight 记录清掉并取消 owner

	close(f.blockCh)
	res := <-done
	if res.err == nil {
		t.Errorf("expected error (creation cancelled by CloseAll), got s=%v err=%v", res.s, res.err)
	}
	if res.s != nil {
		t.Errorf("no session should be produced, got %v", res.s)
	}
	opens, closes := f.counts()
	if opens != 1 {
		t.Errorf("opens = %d, want 1", opens)
	}
	if closes != 0 {
		t.Errorf("closes = %d, want 0 (no session was ever created)", closes)
	}
}

// ---------------------------------------------------------------------------
// ToolCache
// ---------------------------------------------------------------------------

type fakeTool struct{ name string }

func (f fakeTool) Name() string                                { return f.name }
func (f fakeTool) Description() string                         { return "" }
func (f fakeTool) Run(context.Context, string) (string, error) { return "", nil }

func TestToolCacheLazyAndMtime(t *testing.T) {
	loads := 0
	mtime := time.Unix(1000, 0)
	cache := NewToolCache(
		func() ([]capability.Tool, error) {
			loads++
			return []capability.Tool{fakeTool{"t"}}, nil
		},
		func() *time.Time { return &mtime },
		nil,
	)

	if _, err := cache.Initialize(); err != nil {
		t.Fatal(err)
	}
	if loads != 1 {
		t.Errorf("loads = %d, want 1", loads)
	}
	// 幂等。
	if _, err := cache.Initialize(); err != nil {
		t.Fatal(err)
	}
	if loads != 1 {
		t.Errorf("loads = %d after second Initialize, want 1", loads)
	}
	// GetCached 未过期 → 不重载。
	if got := cache.GetCached(); len(got) != 1 {
		t.Errorf("cached = %d", len(got))
	}
	if loads != 1 {
		t.Errorf("loads = %d, want 1 (not stale)", loads)
	}
	// mtime 前进 → 过期 → 重载。
	mtime = mtime.Add(time.Hour)
	cache.GetCached()
	if loads != 2 {
		t.Errorf("loads = %d, want 2 (stale reload)", loads)
	}
}

// ---------------------------------------------------------------------------
// OAuth
// ---------------------------------------------------------------------------

func TestOAuthDoubleCheckedLock(t *testing.T) {
	posts := 0
	clock := time.Unix(0, 0)
	mgr := NewOAuthTokenManager(map[string]McpOAuthConfig{
		"srv": {
			GrantType:    "client_credentials",
			TokenURL:     "http://x/token",
			ClientID:     "id",
			ClientSecret: "sec",
		},
	})
	mgr.now = func() time.Time { return clock }
	mgr.post = func(_ context.Context, _ string, data map[string]string) (map[string]any, error) {
		posts++
		// 校验 client_credentials 表单。
		if data["client_id"] != "id" || data["client_secret"] != "sec" || data["grant_type"] != "client_credentials" {
			t.Errorf("bad form data: %v", data)
		}
		return map[string]any{"access_token": fmt.Sprintf("tok-%d", posts), "expires_in": 3600}, nil
	}

	ctx := context.Background()
	h1, err := mgr.GetAuthorizationHeader(ctx, "srv")
	if err != nil {
		t.Fatal(err)
	}
	if h1 != "Bearer tok-1" {
		t.Errorf("h1 = %q", h1)
	}
	// 未过期 → 缓存,不重新 fetch。
	h2, _ := mgr.GetAuthorizationHeader(ctx, "srv")
	if h2 != "Bearer tok-1" || posts != 1 {
		t.Errorf("h2 = %q, posts = %d (want cached)", h2, posts)
	}
	// 推进到过期(含 refresh_skew)→ 重新 fetch。
	clock = clock.Add(time.Hour)
	h3, _ := mgr.GetAuthorizationHeader(ctx, "srv")
	if h3 != "Bearer tok-2" || posts != 2 {
		t.Errorf("h3 = %q, posts = %d (want refetch)", h3, posts)
	}
	// 无 OAuth 配置的 server → 空头。
	h4, _ := mgr.GetAuthorizationHeader(ctx, "other")
	if h4 != "" {
		t.Errorf("expected empty header for non-oauth server, got %q", h4)
	}
}

func TestOAuthConcurrentSingleFetch(t *testing.T) {
	posts := 0
	mgr := NewOAuthTokenManager(map[string]McpOAuthConfig{
		"srv": {GrantType: "client_credentials", TokenURL: "http://x", ClientID: "i", ClientSecret: "s"},
	})
	mgr.now = time.Now
	mgr.post = func(_ context.Context, _ string, _ map[string]string) (map[string]any, error) {
		posts++
		time.Sleep(20 * time.Millisecond) // 让其它 goroutine 到达锁
		return map[string]any{"access_token": "tok", "expires_in": 3600}, nil
	}

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := mgr.GetAuthorizationHeader(context.Background(), "srv"); err != nil {
				t.Errorf("err: %v", err)
			}
		}()
	}
	wg.Wait()
	if posts != 1 {
		t.Errorf("posts = %d, want 1 (double-checked lock)", posts)
	}
}

func TestBuildTokenRequestData(t *testing.T) {
	if _, err := buildTokenRequestData(McpOAuthConfig{GrantType: "client_credentials"}); err == nil {
		t.Error("expected error for client_credentials without id/secret")
	}
	if _, err := buildTokenRequestData(McpOAuthConfig{GrantType: "refresh_token"}); err == nil {
		t.Error("expected error for refresh_token without token")
	}
	if _, err := buildTokenRequestData(McpOAuthConfig{GrantType: "bogus"}); err == nil {
		t.Error("expected error for unsupported grant type")
	}
}

func TestParseTokenResponse(t *testing.T) {
	tok, err := parseTokenResponse(map[string]any{"access_token": "a", "token_type": "Bearer", "expires_in": 60}, McpOAuthConfig{}, time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	if tok.AccessToken != "a" || tok.TokenType != "Bearer" || tok.ExpiresAt.Unix() != 60 {
		t.Errorf("got %+v", tok)
	}
	// expires_in 非法 → 默认 3600。
	tok2, _ := parseTokenResponse(map[string]any{"access_token": "a", "expires_in": "bogus"}, McpOAuthConfig{}, time.Unix(0, 0))
	if tok2.ExpiresAt.Unix() != 3600 {
		t.Errorf("expires = %d, want 3600", tok2.ExpiresAt.Unix())
	}
	// 缺 token → 报错。
	if _, err := parseTokenResponse(map[string]any{}, McpOAuthConfig{}, time.Unix(0, 0)); err == nil {
		t.Error("expected error for missing access_token")
	}
}

// ---------------------------------------------------------------------------
// 路径改写(localPathFromURI / rewriteLocalPathsInText)
// ---------------------------------------------------------------------------

type fakeWorkspace struct {
	workDir string
	vpath   func(local, tid, uid string) (string, bool)
}

func (w *fakeWorkspace) WorkDir(_, _ string) string { return w.workDir }
func (w *fakeWorkspace) VirtualPath(local, tid, uid string) (string, bool) {
	return w.vpath(local, tid, uid)
}

func TestLocalPathFromURI(t *testing.T) {
	cases := []struct{ uri, base, want string }{
		{"file:///home/x/file.png", "", "/home/x/file.png"},
		{"/abs/file.png", "", "/abs/file.png"},
		{"http://x/file.png", "", ""},
		{"data:image/png;base64,xx", "", ""},
		{"relative/file.png", "/base", "/base/relative/file.png"},
		{"relative/file.png", "", ""},
		{"", "", ""},
	}
	for _, c := range cases {
		if got := localPathFromURI(c.uri, c.base); got != c.want {
			t.Errorf("localPathFromURI(%q, %q) = %q, want %q", c.uri, c.base, got, c.want)
		}
	}
}

func TestRewriteLocalPathsInText(t *testing.T) {
	ws := &fakeWorkspace{
		workDir: "/mnt/user-data/t1",
		vpath: func(local, _, _ string) (string, bool) {
			if strings.HasPrefix(local, "/mnt/user-data/t1/") {
				return "/mnt/user-data/" + strings.TrimPrefix(local, "/mnt/user-data/t1/"), true
			}
			return "", false
		},
	}
	text := "saved as /mnt/user-data/t1/shot.png."
	got := rewriteLocalPathsInText(text, "t1", "u1", "/mnt/user-data/t1", ws)
	if got != "saved as /mnt/user-data/shot.png." {
		t.Errorf("got %q", got)
	}
	// 不可映射的路径原样保留。
	text2 := "outside /elsewhere/x.png"
	if got := rewriteLocalPathsInText(text2, "t1", "u1", "/mnt/user-data/t1", ws); got != text2 {
		t.Errorf("got %q, want unchanged", got)
	}
}

func TestScopeKeyAndThreadID(t *testing.T) {
	if got := ScopeKey("u1", "t1"); got != "u1:t1" {
		t.Errorf("ScopeKey = %q", got)
	}
	rt := &Runtime{Context: map[string]any{"thread_id": "t42"}}
	if got := ExtractThreadID(rt); got != "t42" {
		t.Errorf("thread = %q", got)
	}
	if got := ExtractThreadID(nil); got != "default" {
		t.Errorf("thread default = %q", got)
	}
	if got := ResolveRuntimeUserID(&Runtime{Context: map[string]any{"user_id": "u9"}}); got != "u9" {
		t.Errorf("user = %q", got)
	}
}

// WrapToolsForPooling:stdio 池化,SSE/HTTP 原样。
func TestWrapToolsForPooling(t *testing.T) {
	pool := NewSessionPool(&countingFactory{})
	servers := map[string]map[string]any{
		"stdio-srv": {"transport": "stdio"},
		"sse-srv":   {"transport": "sse"},
	}
	tools := []MCPToolDescriptor{
		{Name: "stdio-srv_tool1", ServerName: "stdio-srv", Connection: map[string]any{"transport": "stdio"}},
		{Name: "sse-srv_tool2", ServerName: "sse-srv", Connection: map[string]any{"transport": "sse"}},
		{Name: "unmatched_tool", Connection: map[string]any{}},
	}
	wrapped := WrapToolsForPooling(tools, servers, pool, nil, nil)
	if len(wrapped) != 3 {
		t.Fatalf("len = %d", len(wrapped))
	}
	if _, ok := wrapped[0].(*SessionPoolTool); !ok {
		t.Errorf("stdio tool should be pooled, got %T", wrapped[0])
	}
	if _, ok := wrapped[1].(*SessionPoolTool); ok {
		t.Errorf("sse tool should NOT be pooled, got %T", wrapped[1])
	}
	if _, ok := wrapped[2].(*SessionPoolTool); ok {
		t.Errorf("unmatched tool should NOT be pooled, got %T", wrapped[2])
	}
	// 前缀剥离。
	pooled := wrapped[0].(*SessionPoolTool)
	if pooled.originalName != "tool1" {
		t.Errorf("originalName = %q, want tool1", pooled.originalName)
	}
}
