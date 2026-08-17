// 「sandbox」能力:两层隔离 + 升级。
//
// 对应 deer-flow:
//   - sandbox/local/local_sandbox.py::LocalSandbox            —— 第一层:进程级(非安全边界)
//   - sandbox/local/local_sandbox_provider.py::LocalSandboxProvider —— LRU 缓存 per-thread 沙盒
//   - community/aio_sandbox/...::AioSandbox + Docker backend  —— 第二层:容器级(真隔离)
//   - sandbox/sandbox_provider.py::get_sandbox_provider       —— provider 单例 + 动态切换「升级」
//   - sandbox/tools.py + security.py                          —— 沙盒工具 + 安全开关
//
// 两层隔离:
//  1. LocalSandbox  —— 直接 exec 子进程,只是「把命令交给宿主 shell」,不是安全边界。
//     deer-flow 里 allow_host_bash 默认 false,路径校验只是 best-effort。
//  2. DockerSandbox —— 命令跑在独立容器里,才是真正的隔离边界。
//
// 「升级」:配置里 sandbox.use 从 "local" 切到 "docker",provider 单例动态换实现,
// 上层 harness/tools 代码一行不改 —— 这就是 capability-seam 的收益。
//
// 关键生产细节(逐行对应源码):
//   - 虚拟路径三层防御:/mnt/user-data 映射(PathMapping)→ 路径穿越校验(_resolve_path
//     的 relative_to 越界检查 + _reject_path_traversal)→ 输出反向掩码
//     (_reverse_resolve_paths_in_output 把本地路径掩回虚拟路径)。
//   - LocalSandboxProvider LRU 缓存(cap=256,访问 move-to-end)。
//   - DockerSandboxProvider warm pool + replicas 软上限 + 孤儿容器 reconcile。
//   - allow_host_bash 默认 false(security.py::is_host_bash_allowed)。
//   - write_file 80KB 限制(tools.py::_WRITE_FILE_CONTENT_MAX_BYTES)防 LLM 流式超时。
package provider

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"deerflow-go/capability"
)

func init() {
	// 注册两层隔离的 provider。名字即「升级开关」。
	// 同时注册完整 class path 别名,等价于 resolve_class("module:Class") 反射。
	capability.RegisterSandboxProvider("local", func() capability.SandboxProvider {
		return NewLocalSandboxProvider(".", DefaultMaxCachedThreadSandboxes)
	})
	capability.RegisterSandboxProvider("deerflow.sandbox.local:LocalSandboxProvider", func() capability.SandboxProvider {
		return NewLocalSandboxProvider(".", DefaultMaxCachedThreadSandboxes)
	})
	capability.RegisterSandboxProvider("docker", func() capability.SandboxProvider {
		return NewDockerSandboxProvider(DefaultSandboxImage, "deer-flow-sandbox", DefaultReplicas, nil)
	})
	capability.RegisterSandboxProvider("deerflow.community.aio_sandbox:AioSandboxProvider", func() capability.SandboxProvider {
		return NewDockerSandboxProvider(DefaultSandboxImage, "deer-flow-sandbox", DefaultReplicas, nil)
	})
}

// 虚拟路径前缀(对应 config/paths.py::VIRTUAL_PATH_PREFIX)。
const (
	VirtualPathPrefix          = "/mnt/user-data"
	AcpWorkspaceVirtualPath    = "/mnt/acp-workspace"
	DefaultSkillsContainerPath = "/mnt/skills"
)

// ---------------------------------------------------------------------------
// 路径映射(对应 local_sandbox.py::PathMapping)
// ---------------------------------------------------------------------------

// PathMapping 一个「容器路径 → 本地路径」映射(可选只读)。
type PathMapping struct {
	ContainerPath string
	LocalPath     string
	ReadOnly      bool
}

// resolvedMapping 是解析后的映射(本地路径已 resolve,只算一次)。
type resolvedMapping struct {
	containerPath string
	localPath     string
	readOnly      bool
}

// errPathTraversal 对应 Python 的 PermissionError(路径越界)。
type errPathTraversal struct{ Path string }

func (e *errPathTraversal) Error() string {
	return "Access denied: path escapes mounted directory: " + e.Path
}

// ---------------------------------------------------------------------------
// 第一层:LocalSandbox(进程级隔离)
// ---------------------------------------------------------------------------

// LocalSandbox 在宿主进程里直接执行命令。
// 注意:这层不是安全边界 —— 对应 deer-flow 里 LocalSandboxProvider 的注释:
// "not a secure isolation boundary for shell access"。
type LocalSandbox struct {
	id           string
	byContainer  []resolvedMapping // 按 container 路径长度降序(前向解析)
	byLocal      []resolvedMapping // 按 local 路径长度降序(反向解析)
	reverseRes   []*regexp.Regexp  // 反向掩码:每个 mapping 一个(缓存,热路径)
	agentWritten map[string]bool   // _agent_written_paths:仅 agent 写过的文件反向掩码
}

// NewLocalSandbox 构造带路径映射的本地沙盒。
func NewLocalSandbox(id string, mappings []PathMapping) *LocalSandbox {
	resolved := make([]resolvedMapping, 0, len(mappings))
	for _, m := range mappings {
		lp := resolveLocalPath(m.LocalPath)
		resolved = append(resolved, resolvedMapping{
			containerPath: m.ContainerPath,
			localPath:     lp,
			readOnly:      m.ReadOnly,
		})
	}
	byContainer := append([]resolvedMapping(nil), resolved...)
	sort.Slice(byContainer, func(i, j int) bool {
		return len(strings.TrimRight(byContainer[i].containerPath, "/")) > len(strings.TrimRight(byContainer[j].containerPath, "/"))
	})
	byLocal := append([]resolvedMapping(nil), resolved...)
	sort.Slice(byLocal, func(i, j int) bool { return len(byLocal[i].localPath) > len(byLocal[j].localPath) })

	s := &LocalSandbox{id: id, byContainer: byContainer, byLocal: byLocal, agentWritten: map[string]bool{}}
	for _, m := range byLocal {
		// 编译反向掩码正则(缓存,对应 _reverse_output_patterns)。
		pattern := regexp.QuoteMeta(m.localPath) + `(?:[/\\][^\s"';&|<>()]*)?`
		s.reverseRes = append(s.reverseRes, regexp.MustCompile(pattern))
	}
	return s
}

func (s *LocalSandbox) ID() string { return s.id }

func resolveLocalPath(p string) string {
	if r, err := filepath.Abs(p); err == nil {
		p = r
	}
	if r, err := filepath.EvalSymlinks(p); err == nil {
		p = r
	}
	// 统一为 forward slash:Path.resolve() 在 Linux 返回 forward slash,这里跨平台对齐,
	// 后续 withinRoot / reverseResolve / 正则掩码全部按 forward slash 比较。
	return path.Clean(filepath.ToSlash(p))
}

// findPathMapping 按 container 路径最长前缀匹配(对应 _find_path_mapping)。
func (s *LocalSandbox) findPathMapping(p string) (resolvedMapping, string, bool) {
	for _, m := range s.byContainer {
		cp := strings.TrimRight(m.containerPath, "/")
		if cp == "" {
			cp = "/"
		}
		if cp == "/" {
			if strings.HasPrefix(p, "/") {
				return m, strings.TrimLeft(p, "/"), true
			}
			continue
		}
		if p == cp || strings.HasPrefix(p, cp+"/") {
			return m, strings.TrimLeft(p[len(cp):], "/"), true
		}
	}
	return resolvedMapping{}, "", false
}

// resolvePathStrict 把容器路径解析成本地路径,越界返回错误(对应 _resolve_path_with_mapping)。
func (s *LocalSandbox) resolvePathStrict(p string) (string, *resolvedMapping, error) {
	m, rel, ok := s.findPathMapping(p)
	if !ok {
		return p, nil, nil
	}
	var resolved string
	if rel == "" {
		resolved = m.localPath
	} else {
		rel = strings.TrimLeft(filepath.ToSlash(rel), "/")
		resolved = path.Clean(strings.TrimRight(m.localPath, "/") + "/" + rel)
	}
	if !withinRoot(m.localPath, resolved) {
		return "", &m, &errPathTraversal{Path: p}
	}
	return resolved, &m, nil
}

// resolvePath 容错解析(对应 _resolve_path:命令内容解析用,失败返回原路径)。
func (s *LocalSandbox) resolvePath(p string) string {
	r, _, err := s.resolvePathStrict(p)
	if err != nil {
		return p
	}
	return r
}

// isReadOnly 判断解析路径是否落在只读挂载下(对应 _is_resolved_path_read_only)。
func (s *LocalSandbox) isReadOnly(m *resolvedMapping, resolvedPath string) bool {
	if m != nil && m.readOnly {
		return true
	}
	best := -1
	ro := false
	for _, mm := range s.byLocal {
		if resolvedPath == mm.localPath || strings.HasPrefix(resolvedPath, mm.localPath+"/") {
			if len(mm.localPath) > best {
				best = len(mm.localPath)
				ro = mm.readOnly
			}
		}
	}
	return ro
}

// reverseResolvePath 本地路径反向解析回容器路径(对应 _reverse_resolve_path)。
func (s *LocalSandbox) reverseResolvePath(p string) string {
	normalized := filepath.ToSlash(p)
	lp := normalized
	if r, err := filepath.EvalSymlinks(filepath.FromSlash(normalized)); err == nil {
		lp = filepath.ToSlash(r)
	}
	lp = path.Clean(lp)
	for _, m := range s.byLocal {
		if lp == m.localPath || strings.HasPrefix(lp, m.localPath+"/") {
			rel := strings.TrimLeft(lp[len(m.localPath):], "/")
			cp := strings.TrimRight(m.containerPath, "/")
			if rel == "" {
				return cp
			}
			return cp + "/" + rel
		}
	}
	return lp
}

// reverseResolveOutput 把输出里的本地路径掩回容器路径(对应 _reverse_resolve_paths_in_output)。
func (s *LocalSandbox) reverseResolveOutput(output string) string {
	result := output
	for _, re := range s.reverseRes {
		result = re.ReplaceAllStringFunc(result, func(match string) string {
			return s.reverseResolvePath(match)
		})
	}
	return result
}

// resolvePathsInCommand 把命令里的容器路径解析成本地路径(对应 _resolve_paths_in_command)。
func (s *LocalSandbox) resolvePathsInCommand(command string) string {
	return replaceContainerPaths(command, s.byContainer, false)
}

// resolvePathsInContent 把文件内容里的容器路径解析成本地路径(对应 _resolve_paths_in_content)。
func (s *LocalSandbox) resolvePathsInContent(content string) string {
	return replaceContainerPaths(content, s.byContainer, true)
}

// ExecuteCommand 在沙盒执行命令,返回 stdout/stderr(含退出码/空输出标记)。
func (s *LocalSandbox) ExecuteCommand(command string) string {
	resolvedCommand := s.resolvePathsInCommand(command)
	shell := detectShell()
	stdout, stderr, exitCode := runShell(shell, resolvedCommand)

	output := stdout
	if stderr != "" {
		if output != "" {
			output += "\nStd Error:\n" + stderr
		} else {
			output = stderr
		}
	}
	if exitCode != 0 {
		output += fmt.Sprintf("\nExit Code: %d", exitCode)
	}
	if output == "" {
		output = "(no output)"
	}
	return s.reverseResolveOutput(output)
}

// ReadFile 读取 UTF-8 文件(仅 agent 写过的文件反向掩码,对应 read_file 的 PR #1935 语义)。
func (s *LocalSandbox) ReadFile(p string) (string, error) {
	resolved, _, err := s.resolvePathStrict(p)
	if err != nil {
		return "", err
	}
	b, err := os.ReadFile(resolved)
	if err != nil {
		return "", fmt.Errorf("read file %s: %w", p, err)
	}
	content := string(b)
	if s.agentWritten[resolved] {
		content = s.reverseResolveOutput(content)
	}
	return content, nil
}

// DownloadFile 下载二进制内容(限制在 /mnt/user-data 下,最大 100MB)。
func (s *LocalSandbox) DownloadFile(p string) ([]byte, error) {
	if err := validateDownloadPath(p); err != nil {
		return nil, err
	}
	resolved, _, err := s.resolvePathStrict(p)
	if err != nil {
		return nil, err
	}
	const maxDownloadSize = 100 * 1024 * 1024
	info, err := os.Stat(resolved)
	if err != nil {
		return nil, fmt.Errorf("download file %s: %w", p, err)
	}
	if info.Size() > maxDownloadSize {
		return nil, fmt.Errorf("file exceeds maximum download size of %d bytes", maxDownloadSize)
	}
	return os.ReadFile(resolved)
}

// ListDir 列出目录内容(最多 maxDepth 层,目录带尾 "/")。
func (s *LocalSandbox) ListDir(p string, maxDepth int) []string {
	resolved := s.resolvePath(p)
	entries := listDirTree(resolved, maxDepth)
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		isDir := strings.HasSuffix(e, "/") || strings.HasSuffix(e, "\\")
		reversed := s.reverseResolvePath(strings.TrimRight(e, "/\\"))
		if isDir && !strings.HasSuffix(reversed, "/") {
			reversed += "/"
		}
		out = append(out, reversed)
	}
	return out
}

// WriteFile 写文件(全量覆盖或追加),只读挂载拒绝,并追踪 agent 写过的路径。
func (s *LocalSandbox) WriteFile(p, content string, appendMode bool) error {
	resolved, mapping, err := s.resolvePathStrict(p)
	if err != nil {
		return err
	}
	if s.isReadOnly(mapping, resolved) {
		return fmt.Errorf("read-only file system: %s", p)
	}
	dir := filepath.Dir(resolved)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("write file %s: %w", p, err)
	}
	resolvedContent := s.resolvePathsInContent(content)
	flag := os.O_WRONLY | os.O_CREATE
	if appendMode {
		flag |= os.O_APPEND
	} else {
		flag |= os.O_TRUNC
	}
	f, err := os.OpenFile(resolved, flag, 0o644)
	if err != nil {
		return fmt.Errorf("write file %s: %w", p, err)
	}
	if _, err := f.WriteString(resolvedContent); err != nil {
		f.Close()
		return fmt.Errorf("write file %s: %w", p, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("write file %s: %w", p, err)
	}
	s.agentWritten[resolved] = true
	return nil
}

// UpdateFile 以二进制内容更新文件(对应 update_file)。
func (s *LocalSandbox) UpdateFile(p string, content []byte) error {
	resolved, mapping, err := s.resolvePathStrict(p)
	if err != nil {
		return err
	}
	if s.isReadOnly(mapping, resolved) {
		return fmt.Errorf("read-only file system: %s", p)
	}
	if err := os.MkdirAll(filepath.Dir(resolved), 0o755); err != nil {
		return err
	}
	return os.WriteFile(resolved, content, 0o644)
}

// Glob 匹配路径(对应 glob)。
func (s *LocalSandbox) Glob(p, pattern string, includeDirs bool, maxResults int) ([]string, bool, error) {
	resolved := s.resolvePath(p)
	matches, truncated, err := findGlobMatches(resolved, pattern, includeDirs, maxResults)
	if err != nil {
		return nil, false, err
	}
	out := make([]string, 0, len(matches))
	for _, m := range matches {
		out = append(out, s.reverseResolvePath(m))
	}
	return out, truncated, nil
}

// Grep 搜索匹配行(对应 grep)。
func (s *LocalSandbox) Grep(p, pattern, glob string, literal, caseSensitive bool, maxResults int) ([]capability.GrepMatch, bool, error) {
	resolved := s.resolvePath(p)
	matches, truncated, err := findGrepMatches(resolved, pattern, glob, literal, caseSensitive, maxResults, DefaultMaxFileSizeBytes, DefaultLineSummaryLength)
	if err != nil {
		return nil, false, err
	}
	out := make([]capability.GrepMatch, 0, len(matches))
	for _, m := range matches {
		out = append(out, capability.GrepMatch{
			Path:       s.reverseResolvePath(m.Path),
			LineNumber: m.LineNumber,
			Line:       m.Line,
		})
	}
	return out, truncated, nil
}

// ---------------------------------------------------------------------------
// 路径替换辅助(对应 _resolve_paths_in_command / _content 的简化等价)
// ---------------------------------------------------------------------------

func replaceContainerPaths(input string, mappings []resolvedMapping, contentMode bool) string {
	result := input
	for _, m := range mappings {
		cp := strings.TrimRight(m.containerPath, "/")
		if cp == "" || cp == "/" {
			continue
		}
		result = replaceOneContainerPath(result, cp, m.localPath, contentMode)
	}
	return result
}

func replaceOneContainerPath(input, cp, localRoot string, contentMode bool) string {
	var b strings.Builder
	i := 0
	for i < len(input) {
		idx := strings.Index(input[i:], cp)
		if idx < 0 {
			b.WriteString(input[i:])
			break
		}
		start := i + idx
		end := start + len(cp)
		rel := ""
		if end < len(input) && input[end] == '/' {
			j := end + 1
			for j < len(input) && !isPathBoundary(input[j]) {
				j++
			}
			rel = input[end+1 : j]
			end = j
		} else if end < len(input) && !isPathBoundary(input[end]) {
			// 前缀后跟非边界且非 '/'(如 /mnt/user-data-extra):跳过。
			b.WriteString(input[i:end])
			i = end
			continue
		}
		resolved := strings.TrimRight(localRoot, "/")
		if rel != "" {
			resolved += "/" + strings.TrimLeft(filepath.ToSlash(rel), "/")
		}
		b.WriteString(input[i:start])
		b.WriteString(resolved)
		i = end
	}
	return b.String()
}

func isPathBoundary(c byte) bool {
	switch c {
	case ' ', '\t', '\n', '\r', '"', '\'', ';', '&', '|', '<', '>', '(', ')':
		return true
	}
	return false
}

func withinRoot(root, p string) bool {
	root = path.Clean(filepath.ToSlash(root))
	p = path.Clean(filepath.ToSlash(p))
	if p == root {
		return true
	}
	return strings.HasPrefix(p, strings.TrimRight(root, "/")+"/")
}

func validateDownloadPath(p string) error {
	normalized := strings.ReplaceAll(p, "\\", "/")
	stripped := strings.TrimLeft(normalized, "/")
	allowed := strings.TrimLeft(VirtualPathPrefix, "/")
	if stripped != allowed && !strings.HasPrefix(stripped, allowed+"/") {
		return fmt.Errorf("Access denied: path must be under '%s'", VirtualPathPrefix)
	}
	return nil
}

// ---------------------------------------------------------------------------
// 目录遍历 + glob/grep 搜索(对应 list_dir.py + search.py)
// ---------------------------------------------------------------------------

const (
	DefaultMaxFileSizeBytes  = 1_000_000
	DefaultLineSummaryLength = 200
)

var ignorePatterns = []string{
	".git", ".svn", ".hg", ".bzr", "node_modules", "__pycache__", ".venv", "venv",
	".env", "env", ".tox", ".nox", ".eggs", "*.egg-info", "site-packages", "dist",
	"build", ".next", ".nuxt", ".output", ".turbo", "target", "out", ".idea", ".vscode",
	"*.swp", "*.swo", "*~", ".project", ".classpath", ".settings", ".DS_Store",
	"Thumbs.db", "desktop.ini", "*.lnk", "*.log", "*.tmp", "*.temp", "*.bak",
	"*.cache", ".cache", "logs", ".coverage", "coverage", ".nyc_output", "htmlcov",
	".pytest_cache", ".mypy_cache", ".ruff_cache",
}

func shouldIgnoreName(name string) bool {
	lower := strings.ToLower(name)
	for _, p := range ignorePatterns {
		if ok, _ := path.Match(p, lower); ok {
			return true
		}
	}
	return false
}

func pathMatches(pattern, relPath string) bool {
	pattern = strings.ReplaceAll(pattern, "\\", "/")
	relPath = strings.ReplaceAll(relPath, "\\", "/")
	relPath = strings.TrimPrefix(relPath, "./")
	return matchSegments(strings.Split(pattern, "/"), strings.Split(relPath, "/"))
}

func matchSegments(p, n []string) bool {
	for len(p) > 0 {
		if p[0] == "**" {
			if matchSegments(p[1:], n) {
				return true
			}
			if len(n) == 0 {
				return false
			}
			n = n[1:]
			continue
		}
		if len(n) == 0 {
			return false
		}
		ok, err := path.Match(p[0], n[0])
		if err != nil || !ok {
			return false
		}
		p = p[1:]
		n = n[1:]
	}
	return len(n) == 0
}

// listDirTree 递归列出目录(最多 maxDepth 层,目录尾 "/"),对应 list_dir.py。
func listDirTree(root string, maxDepth int) []string {
	if maxDepth <= 0 {
		maxDepth = 2
	}
	var result []string
	var traverse func(cur string, depth int)
	traverse = func(cur string, depth int) {
		if depth > maxDepth {
			return
		}
		entries, err := os.ReadDir(cur)
		if err != nil {
			return
		}
		for _, e := range entries {
			if shouldIgnoreName(e.Name()) {
				continue
			}
			full := filepath.Join(cur, e.Name())
			if e.Type()&os.ModeSymlink != 0 {
				rp, err := filepath.EvalSymlinks(full)
				if err != nil || !withinRoot(resolveLocalPath(root), rp) {
					continue
				}
				if info, err := os.Stat(rp); err == nil && info.IsDir() {
					result = append(result, rp+"/")
				} else {
					result = append(result, rp)
				}
				continue
			}
			if e.IsDir() {
				result = append(result, full+"/")
				if depth < maxDepth {
					traverse(full, depth+1)
				}
			} else {
				result = append(result, full)
			}
		}
	}
	traverse(root, 1)
	sort.Strings(result)
	return result
}

// findGlobMatches 在 root 下按 glob 匹配(对应 find_glob_matches)。
func findGlobMatches(root, pattern string, includeDirs bool, maxResults int) ([]string, bool, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, false, err
	}
	info, err := os.Stat(abs)
	if err != nil {
		return nil, false, err
	}
	if !info.IsDir() {
		return nil, false, fmt.Errorf("path is not a directory: %s", root)
	}

	var matches []string
	truncated := false
	var walk func(cur string) bool
	walk = func(cur string) bool {
		entries, err := os.ReadDir(cur)
		if err != nil {
			return false
		}
		var dirs, files []os.DirEntry
		for _, e := range entries {
			if shouldIgnoreName(e.Name()) {
				continue
			}
			if e.IsDir() {
				dirs = append(dirs, e)
			} else {
				files = append(files, e)
			}
		}
		if includeDirs {
			for _, d := range dirs {
				full := filepath.Join(cur, d.Name())
				rel, _ := filepath.Rel(abs, full)
				if pathMatches(pattern, filepath.ToSlash(rel)) {
					matches = append(matches, full)
					if len(matches) >= maxResults {
						truncated = true
						return true
					}
				}
			}
		}
		for _, f := range files {
			full := filepath.Join(cur, f.Name())
			rel, _ := filepath.Rel(abs, full)
			if pathMatches(pattern, filepath.ToSlash(rel)) {
				matches = append(matches, full)
				if len(matches) >= maxResults {
					truncated = true
					return true
				}
			}
		}
		for _, d := range dirs {
			if walk(filepath.Join(cur, d.Name())) {
				return true
			}
		}
		return false
	}
	walk(abs)
	return matches, truncated, nil
}

// findGrepMatches 在目录内搜索匹配行(对应 find_grep_matches)。
// 注意:Go 的 regexp 是 RE2(线性时间,无回溯),天然无 ReDoS,但保留 deer-flow 的
// 行长跳过(_max_line_chars)与二进制/大文件/符号链接跳过语义。
func findGrepMatches(root, pattern, globPattern string, literal, caseSensitive bool, maxResults, maxFileSize, lineSummaryLen int) ([]capability.GrepMatch, bool, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, false, err
	}
	info, err := os.Stat(abs)
	if err != nil {
		return nil, false, err
	}
	if !info.IsDir() {
		return nil, false, fmt.Errorf("path is not a directory: %s", root)
	}

	regexSource := pattern
	if literal {
		regexSource = regexp.QuoteMeta(pattern)
	}
	if !caseSensitive {
		regexSource = "(?i)" + regexSource
	}
	re, err := regexp.Compile(regexSource)
	if err != nil {
		return nil, false, err
	}
	maxLineChars := lineSummaryLen * 10

	var matches []capability.GrepMatch
	truncated := false
	stopErr := fmt.Errorf("stop")

	err = filepath.WalkDir(abs, func(current string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return nil // 对应 OSError -> continue
		}
		if current == abs {
			return nil
		}
		if d.IsDir() {
			if shouldIgnoreName(d.Name()) {
				return filepath.SkipDir
			}
			return nil
		}
		if shouldIgnoreName(d.Name()) {
			return nil
		}
		rel, _ := filepath.Rel(abs, current)
		relPath := filepath.ToSlash(rel)
		if globPattern != "" && !pathMatches(globPattern, relPath) {
			return nil
		}
		if d.Type()&os.ModeSymlink != 0 {
			return nil
		}
		finfo, err := d.Info()
		if err != nil || finfo.Size() > int64(maxFileSize) {
			return nil
		}
		if isBinaryFile(current) {
			return nil
		}
		f, err := os.Open(current)
		if err != nil {
			return nil
		}
		defer f.Close()
		r := bufio.NewReader(f)
		lineNo := 0
		for {
			line, rerr := r.ReadString('\n')
			if len(line) > 0 {
				lineNo++
				line = strings.TrimRight(line, "\n")
				line = strings.TrimRight(line, "\r")
				if len(line) <= maxLineChars && re.MatchString(line) {
					matches = append(matches, capability.GrepMatch{
						Path:       current,
						LineNumber: lineNo,
						Line:       truncateLine(line, lineSummaryLen),
					})
					if len(matches) >= maxResults {
						truncated = true
						return stopErr
					}
				}
			}
			if rerr == io.EOF {
				break
			}
			if rerr != nil {
				break
			}
		}
		return nil
	})
	if err != nil && err != stopErr {
		return nil, false, err
	}
	return matches, truncated, nil
}

func truncateLine(line string, maxChars int) string {
	if len(line) <= maxChars {
		return line
	}
	if maxChars < 3 {
		return line[:maxChars]
	}
	return line[:maxChars-3] + "..."
}

func isBinaryFile(p string) bool {
	f, err := os.Open(p)
	if err != nil {
		return true
	}
	defer f.Close()
	buf := make([]byte, 8192)
	n, _ := f.Read(buf)
	return bytes.IndexByte(buf[:n], 0) >= 0
}

// ---------------------------------------------------------------------------
// shell 检测(对应 local_sandbox.py::_get_shell)
// ---------------------------------------------------------------------------

func shellName(shell string) string {
	shell = strings.ReplaceAll(shell, "\\", "/")
	idx := strings.LastIndex(shell, "/")
	if idx >= 0 {
		shell = shell[idx+1:]
	}
	return strings.ToLower(shell)
}

func isPowerShell(shell string) bool {
	n := shellName(shell)
	return n == "powershell" || n == "powershell.exe" || n == "pwsh" || n == "pwsh.exe"
}

func isCmdShell(shell string) bool {
	n := shellName(shell)
	return n == "cmd" || n == "cmd.exe"
}

func detectShell() string {
	for _, s := range []string{"/bin/zsh", "/bin/bash", "/bin/sh"} {
		if info, err := os.Stat(s); err == nil && !info.IsDir() {
			return s
		}
	}
	for _, s := range []string{"sh", "bash", "zsh"} {
		if p, err := exec.LookPath(s); err == nil {
			return p
		}
	}
	if runtime.GOOS == "windows" {
		for _, s := range []string{"pwsh", "powershell", "cmd.exe"} {
			if p, err := exec.LookPath(s); err == nil {
				return p
			}
		}
		return "cmd.exe"
	}
	return "sh"
}

func runShell(shell, command string) (stdout, stderr string, exitCode int) {
	var args []string
	switch {
	case isPowerShell(shell):
		args = []string{"-NoProfile", "-Command", command}
	case isCmdShell(shell):
		args = []string{"/c", command}
	default:
		args = []string{"-c", command}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, shell, args...)
	var outBuf, errBuf bytes.Buffer
	cmd.Stdout = &outBuf
	cmd.Stderr = &errBuf
	err := cmd.Run()
	exitCode = 0
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			exitCode = ee.ExitCode()
		} else {
			exitCode = -1
		}
	}
	return outBuf.String(), errBuf.String(), exitCode
}

// ---------------------------------------------------------------------------
// LocalSandboxProvider(LRU 缓存)
// 对应 local_sandbox_provider.py:per-thread 沙盒 + LRU cap + 泛型单例。
// ---------------------------------------------------------------------------

// DefaultMaxCachedThreadSandboxes 每 provider 缓存 per-thread 沙盒的上限。
const DefaultMaxCachedThreadSandboxes = 256

// LocalSandboxProvider 实现 acquire/get/release/reset 生命周期。
type LocalSandboxProvider struct {
	workdir  string
	maxCache int
	mu       sync.Mutex
	generic  *LocalSandbox
	threads  map[string]*LocalSandbox
	order    []string // LRU 顺序(尾部 = 最近使用)
}

// NewLocalSandboxProvider 构造 provider。workdir 是进程级工作目录。
func NewLocalSandboxProvider(workdir string, maxCached int) *LocalSandboxProvider {
	if maxCached <= 0 {
		maxCached = DefaultMaxCachedThreadSandboxes
	}
	return &LocalSandboxProvider{
		workdir:  workdir,
		maxCache: maxCached,
		threads:  map[string]*LocalSandbox{},
	}
}

func (p *LocalSandboxProvider) touchLocked(threadID string) {
	for i, id := range p.order {
		if id == threadID {
			p.order = append(p.order[:i], p.order[i+1:]...)
			break
		}
	}
	p.order = append(p.order, threadID)
}

func (p *LocalSandboxProvider) evictLocked() {
	for len(p.threads) > p.maxCache && len(p.order) > 0 {
		oldest := p.order[0]
		p.order = p.order[1:]
		delete(p.threads, oldest)
	}
}

// Acquire 返回 per-thread 沙盒 id(threadID 空则返回泛型单例 "local")。
func (p *LocalSandboxProvider) Acquire(threadID string) (string, error) {
	if threadID == "" {
		p.mu.Lock()
		defer p.mu.Unlock()
		if p.generic == nil {
			p.generic = NewLocalSandbox("local", []PathMapping{{ContainerPath: "/", LocalPath: p.workdir}})
		}
		return p.generic.ID(), nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if sb, ok := p.threads[threadID]; ok {
		p.touchLocked(threadID)
		return sb.ID(), nil
	}
	// per-thread 沙盒:映射 /mnt/user-data/... 到线程工作目录。
	base := filepath.Join(p.workdir, "threads", threadID)
	mappings := []PathMapping{
		{ContainerPath: VirtualPathPrefix, LocalPath: filepath.Join(base, "user-data")},
		{ContainerPath: VirtualPathPrefix + "/workspace", LocalPath: filepath.Join(base, "user-data", "workspace")},
		{ContainerPath: VirtualPathPrefix + "/uploads", LocalPath: filepath.Join(base, "user-data", "uploads")},
		{ContainerPath: VirtualPathPrefix + "/outputs", LocalPath: filepath.Join(base, "user-data", "outputs")},
		{ContainerPath: AcpWorkspaceVirtualPath, LocalPath: filepath.Join(base, "acp-workspace")},
	}
	sb := NewLocalSandbox("local:"+threadID, mappings)
	p.threads[threadID] = sb
	p.order = append(p.order, threadID)
	p.evictLocked()
	return sb.ID(), nil
}

// Get 按 id 取回沙盒。
func (p *LocalSandboxProvider) Get(id string) (capability.Sandbox, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if id == "local" {
		return p.generic, p.generic != nil
	}
	if strings.HasPrefix(id, "local:") {
		threadID := strings.TrimPrefix(id, "local:")
		sb, ok := p.threads[threadID]
		if ok {
			p.touchLocked(threadID)
		}
		return sb, ok
	}
	return nil, false
}

// Release 本地沙盒无资源释放,保留缓存(对应 release 的 pass 语义)。
func (p *LocalSandboxProvider) Release(id string) {}

// Reset 丢弃所有缓存(对应 reset:config 变更后下次 acquire 重建)。
func (p *LocalSandboxProvider) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.generic = nil
	p.threads = map[string]*LocalSandbox{}
	p.order = nil
}

// Shutdown 与 reset 同路径(对应 shutdown)。
func (p *LocalSandboxProvider) Shutdown() { p.Reset() }

// ---------------------------------------------------------------------------
// 第二层:DockerSandbox(容器级隔离)
// ---------------------------------------------------------------------------

// DefaultSandboxImage / DefaultReplicas 对应 aio_sandbox_provider.py 的默认值。
const (
	DefaultSandboxImage = "deerflow-sandbox:latest"
	DefaultReplicas     = 3
)

// DockerSandbox 把命令跑进独立容器(真实 docker CLI)。
type DockerSandbox struct {
	id        string
	container string
}

func (s *DockerSandbox) ID() string { return s.id }

func (s *DockerSandbox) ExecuteCommand(command string) string {
	cmd := exec.Command("docker", "exec", s.container, "sh", "-c", command)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Sprintf("Error: %v", err)
	}
	output := string(out)
	if output == "" {
		return "(no output)"
	}
	return output
}

func (s *DockerSandbox) ReadFile(p string) (string, error) {
	cmd := exec.Command("docker", "exec", s.container, "cat", p)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("read file %s: %v", p, err)
	}
	return string(out), nil
}

func (s *DockerSandbox) DownloadFile(p string) ([]byte, error) {
	if err := validateDownloadPath(p); err != nil {
		return nil, err
	}
	cmd := exec.Command("docker", "exec", s.container, "cat", p)
	return cmd.Output()
}

func (s *DockerSandbox) ListDir(p string, maxDepth int) []string {
	cmd := exec.Command("docker", "exec", s.container, "sh", "-c",
		fmt.Sprintf("find %s -maxdepth %d -type f -o -type d 2>/dev/null | head -500", shellQuote(p), maxDepth))
	out, err := cmd.Output()
	if err != nil {
		return nil
	}
	var result []string
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			result = append(result, line)
		}
	}
	return result
}

func (s *DockerSandbox) WriteFile(p, content string, appendMode bool) error {
	if appendMode {
		existing, err := s.ReadFile(p)
		if err == nil {
			content = existing + content
		}
	}
	cmd := exec.Command("docker", "exec", "-i", s.container, "sh", "-c", "cat > "+shellQuote(p))
	cmd.Stdin = strings.NewReader(content)
	return cmd.Run()
}

func (s *DockerSandbox) Glob(p, pattern string, includeDirs bool, maxResults int) ([]string, bool, error) {
	if !includeDirs {
		cmd := exec.Command("docker", "exec", s.container, "sh", "-c",
			fmt.Sprintf("find %s -path %s -type f 2>/dev/null | head -%d", shellQuote(p), shellQuote(pattern), maxResults+1))
		out, _ := cmd.Output()
		files := strings.Fields(string(out))
		truncated := len(files) > maxResults
		if truncated {
			files = files[:maxResults]
		}
		return files, truncated, nil
	}
	// include_dirs:简化为 find 所有路径。
	cmd := exec.Command("docker", "exec", s.container, "sh", "-c",
		fmt.Sprintf("find %s -path %s 2>/dev/null | head -%d", shellQuote(p), shellQuote(pattern), maxResults+1))
	out, _ := cmd.Output()
	entries := strings.Fields(string(out))
	truncated := len(entries) > maxResults
	if truncated {
		entries = entries[:maxResults]
	}
	return entries, truncated, nil
}

func (s *DockerSandbox) Grep(p, pattern, glob string, literal, caseSensitive bool, maxResults int) ([]capability.GrepMatch, bool, error) {
	args := []string{"exec", s.container, "grep", "-n"}
	if !caseSensitive {
		args = append(args, "-i")
	}
	if literal {
		args = append(args, "-F")
	}
	args = append(args, "-r", pattern, p)
	cmd := exec.Command("docker", args...)
	out, _ := cmd.Output()
	var matches []capability.GrepMatch
	truncated := false
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, ":", 3)
		lineNo := 0
		content := line
		if len(parts) >= 3 {
			lineNo, _ = strconv.Atoi(parts[1])
			content = parts[2]
		}
		matches = append(matches, capability.GrepMatch{LineNumber: lineNo, Line: truncateLine(content, DefaultLineSummaryLength)})
		if len(matches) >= maxResults {
			truncated = true
			break
		}
	}
	return matches, truncated, nil
}

func (s *DockerSandbox) UpdateFile(p string, content []byte) error {
	cmd := exec.Command("docker", "exec", "-i", s.container, "sh", "-c", "cat > "+shellQuote(p))
	cmd.Stdin = bytes.NewReader(content)
	return cmd.Run()
}

// shellQuote 转义 shell 参数(对应 shlex.quote 的简化)。
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// ---------------------------------------------------------------------------
// DockerSandboxProvider(warm pool + replicas 软上限 + 孤儿 reconcile)
// 对应 aio_sandbox_provider.py 的核心状态机(简化了异步/文件锁/后端抽象)。
// ---------------------------------------------------------------------------

type sandboxInfo struct {
	ID        string
	URL       string
	CreatedAt float64
}

type warmEntry struct {
	info       sandboxInfo
	releasedAt float64
}

// DockerSandboxProvider 管理容器生命周期:warm pool 复用、replicas 软上限、孤儿 reconcile。
type DockerSandboxProvider struct {
	mu              sync.Mutex
	sandboxes       map[string]*DockerSandbox
	infos           map[string]sandboxInfo
	threadSandboxes map[string]string
	lastActivity    map[string]float64
	warmPool        map[string]warmEntry
	shutdownCalled  bool

	image    string
	prefix   string
	replicas int
	backend  containerBackend
}

// NewDockerSandboxProvider 构造 provider。backend 为 nil 时用真实 docker CLI 后端。
func NewDockerSandboxProvider(image, prefix string, replicas int, backend containerBackend) *DockerSandboxProvider {
	if image == "" {
		image = DefaultSandboxImage
	}
	if prefix == "" {
		prefix = "deer-flow-sandbox"
	}
	if replicas <= 0 {
		replicas = DefaultReplicas
	}
	if backend == nil {
		backend = &dockerBackend{}
	}
	p := &DockerSandboxProvider{
		sandboxes:       map[string]*DockerSandbox{},
		infos:           map[string]sandboxInfo{},
		threadSandboxes: map[string]string{},
		lastActivity:    map[string]float64{},
		warmPool:        map[string]warmEntry{},
		image:           image,
		prefix:          prefix,
		replicas:        replicas,
		backend:         backend,
	}
	p.reconcileOrphans()
	return p
}

// deterministicSandboxID 从 thread_id 派生确定性 id(对应 _deterministic_sandbox_id)。
func deterministicSandboxID(threadID string) string {
	sum := sha256.Sum256([]byte(threadID))
	return hex.EncodeToString(sum[:])[:8]
}

// containerName 由前缀 + 确定性 id 构成(对应 Docker 容器命名)。
func (p *DockerSandboxProvider) containerName(sandboxID string) string {
	return p.prefix + "-" + sandboxID
}

// Acquire 获取(或创建)沙盒,返回 id。
func (p *DockerSandboxProvider) Acquire(threadID string) (string, error) {
	// 1. 复用进程内已追踪的沙盒。
	if id, ok := p.reuseInProcess(threadID); ok {
		return id, nil
	}
	// 2. 确定性(或随机)id。
	sandboxID := p.sandboxIDForThread(threadID)
	// 3. 从 warm pool 回收(容器还在跑,免冷启动)。
	if id, ok := p.reclaimWarmPool(threadID, sandboxID); ok {
		return id, nil
	}
	// 4. 创建(受 replicas 软上限约束)。
	return p.createSandbox(threadID, sandboxID)
}

func (p *DockerSandboxProvider) sandboxIDForThread(threadID string) string {
	if threadID == "" {
		return fmt.Sprintf("%08d", time.Now().UnixNano()%100000000)
	}
	return deterministicSandboxID(threadID)
}

func (p *DockerSandboxProvider) reuseInProcess(threadID string) (string, bool) {
	if threadID == "" {
		return "", false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	id, ok := p.threadSandboxes[threadID]
	if !ok {
		return "", false
	}
	if _, alive := p.sandboxes[id]; !alive {
		delete(p.threadSandboxes, threadID)
		return "", false
	}
	p.lastActivity[id] = nowFloat()
	return id, true
}

func (p *DockerSandboxProvider) reclaimWarmPool(threadID, sandboxID string) (string, bool) {
	if threadID == "" {
		return "", false
	}
	p.mu.Lock()
	w, ok := p.warmPool[sandboxID]
	if !ok {
		p.mu.Unlock()
		return "", false
	}
	delete(p.warmPool, sandboxID)
	container := p.containerName(sandboxID)
	p.sandboxes[sandboxID] = &DockerSandbox{id: sandboxID, container: container}
	p.infos[sandboxID] = w.info
	p.lastActivity[sandboxID] = nowFloat()
	p.threadSandboxes[threadID] = sandboxID
	p.mu.Unlock()
	return sandboxID, true
}

func (p *DockerSandboxProvider) createSandbox(threadID, sandboxID string) (string, error) {
	// replicas 软上限:只回收 warm pool 腾额度,不强制停活跃容器。
	p.mu.Lock()
	total := len(p.sandboxes) + len(p.warmPool)
	evicted := ""
	if total >= p.replicas {
		evicted = p.evictOldestWarmLocked()
	}
	p.mu.Unlock()

	container := p.containerName(sandboxID)
	info := sandboxInfo{ID: sandboxID, URL: "http://" + container + ":8080", CreatedAt: nowFloat()}

	// 真实实现:backend.create(docker run 冷启动)。若失败则视为启动失败。
	if err := p.backend.create(container, p.image); err != nil {
		_ = evicted
		return "", fmt.Errorf("sandbox %s failed to start: %w", sandboxID, err)
	}

	p.mu.Lock()
	p.sandboxes[sandboxID] = &DockerSandbox{id: sandboxID, container: container}
	p.infos[sandboxID] = info
	p.lastActivity[sandboxID] = nowFloat()
	if threadID != "" {
		p.threadSandboxes[threadID] = sandboxID
	}
	p.mu.Unlock()
	return sandboxID, nil
}

func (p *DockerSandboxProvider) evictOldestWarmLocked() string {
	if len(p.warmPool) == 0 {
		return ""
	}
	oldestID := ""
	var oldestTs float64
	for id, w := range p.warmPool {
		if oldestID == "" || w.releasedAt < oldestTs {
			oldestID, oldestTs = id, w.releasedAt
		}
	}
	info := p.warmPool[oldestID].info
	delete(p.warmPool, oldestID)
	_ = p.backend.destroy(p.containerName(info.ID))
	return oldestID
}

// reconcileOrphans 启动时把孤儿容器(前进程遗留)收养进 warm pool。
func (p *DockerSandboxProvider) reconcileOrphans() {
	running, err := p.backend.listRunning(p.prefix)
	if err != nil {
		return
	}
	now := nowFloat()
	for _, c := range running {
		p.mu.Lock()
		if _, ok := p.sandboxes[c.ID]; ok {
			p.mu.Unlock()
			continue
		}
		if _, ok := p.warmPool[c.ID]; ok {
			p.mu.Unlock()
			continue
		}
		p.warmPool[c.ID] = warmEntry{info: c, releasedAt: now}
		p.mu.Unlock()
	}
}

// Get 按 id 取回沙盒(更新最近活动时间)。
func (p *DockerSandboxProvider) Get(id string) (capability.Sandbox, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	sb, ok := p.sandboxes[id]
	if ok {
		p.lastActivity[id] = nowFloat()
	}
	return sb, ok
}

// Release 释放沙盒进 warm pool(容器保持运行,供下次 reclaim)。
func (p *DockerSandboxProvider) Release(id string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	sb, ok := p.sandboxes[id]
	if !ok {
		return
	}
	delete(p.sandboxes, id)
	info, hasInfo := p.infos[id]
	delete(p.infos, id)
	for tid, sid := range p.threadSandboxes {
		if sid == id {
			delete(p.threadSandboxes, tid)
		}
	}
	delete(p.lastActivity, id)
	if hasInfo {
		if _, exists := p.warmPool[id]; !exists {
			p.warmPool[id] = warmEntry{info: info, releasedAt: nowFloat()}
		}
	}
	_ = sb
}

// Reset 清空进程内追踪(不销毁容器,对应 aio provider 的 reset 无实现)。
func (p *DockerSandboxProvider) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sandboxes = map[string]*DockerSandbox{}
	p.infos = map[string]sandboxInfo{}
	p.threadSandboxes = map[string]string{}
	p.lastActivity = map[string]float64{}
	p.warmPool = map[string]warmEntry{}
}

// Shutdown 销毁所有 active + warm-pool 容器(幂等)。
func (p *DockerSandboxProvider) Shutdown() {
	p.mu.Lock()
	if p.shutdownCalled {
		p.mu.Unlock()
		return
	}
	p.shutdownCalled = true
	var all []sandboxInfo
	for _, info := range p.infos {
		all = append(all, info)
	}
	for _, w := range p.warmPool {
		all = append(all, w.info)
	}
	p.sandboxes = map[string]*DockerSandbox{}
	p.infos = map[string]sandboxInfo{}
	p.threadSandboxes = map[string]string{}
	p.lastActivity = map[string]float64{}
	p.warmPool = map[string]warmEntry{}
	p.mu.Unlock()

	for _, info := range all {
		_ = p.backend.destroy(p.containerName(info.ID))
	}
}

func nowFloat() float64 { return float64(time.Now().UnixNano()) / 1e9 }

// ---------------------------------------------------------------------------
// Docker CLI 后端(真实 docker;无 docker 环境时返回错误,逻辑仍可测)
// ---------------------------------------------------------------------------

// containerBackend 是容器生命周期后端抽象(测试可注入 mock)。
type containerBackend interface {
	create(containerName, image string) error
	destroy(containerName string) error
	listRunning(prefix string) ([]sandboxInfo, error)
}

// dockerBackend 是真实 docker CLI 后端(直接调用 docker 命令)。
type dockerBackend struct{}

func (dockerBackend) create(containerName, image string) error {
	return exec.Command("docker", "run", "-d", "--name", containerName, image).Run()
}

func (dockerBackend) destroy(containerName string) error {
	return exec.Command("docker", "rm", "-f", containerName).Run()
}

func (dockerBackend) listRunning(prefix string) ([]sandboxInfo, error) {
	out, err := exec.Command("docker", "ps", "--filter", "name="+prefix, "--format", "{{.Names}}").Output()
	if err != nil {
		return nil, err
	}
	var result []sandboxInfo
	for _, name := range strings.Fields(string(out)) {
		id := strings.TrimPrefix(name, prefix+"-")
		result = append(result, sandboxInfo{ID: id, URL: "http://" + name + ":8080", CreatedAt: nowFloat()})
	}
	return result, nil
}

// ---------------------------------------------------------------------------
// 安全开关(对应 security.py)
// ---------------------------------------------------------------------------

// LocalHostBashDisabledMessage 对应 LOCAL_HOST_BASH_DISABLED_MESSAGE。
const LocalHostBashDisabledMessage = "Host bash execution is disabled for LocalSandboxProvider because it is not a secure sandbox boundary. Switch to AioSandboxProvider for isolated bash access, or set sandbox.allow_host_bash: true only in a fully trusted local environment."

// isLocalSandboxID 判断沙盒 id 是否本地沙盒(对应 is_local_sandbox)。
func isLocalSandboxID(id string) bool {
	return id == "local" || strings.HasPrefix(id, "local:")
}

// rejectPathTraversal 拒绝含 ".." 段的路径(对应 _reject_path_traversal)。
func rejectPathTraversal(p string) error {
	normalized := strings.ReplaceAll(p, "\\", "/")
	for _, seg := range strings.Split(normalized, "/") {
		if seg == ".." {
			return fmt.Errorf("Access denied: path traversal detected in '%s'", p)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// 沙盒工具(对应 sandbox/tools.py 的 bash/ls/glob/grep/read_file/write_file)
// ---------------------------------------------------------------------------

// ensureSandbox 懒初始化沙盒(对应 ensure_sandbox_initialized)。
func ensureSandbox(provider capability.SandboxProvider, threadID string) (capability.Sandbox, string, error) {
	id, err := provider.Acquire(threadID)
	if err != nil {
		return nil, "", err
	}
	sb, ok := provider.Get(id)
	if !ok {
		return nil, "", fmt.Errorf("sandbox %q not found after acquisition", id)
	}
	return sb, id, nil
}

// bashTool 对应 bash_tool。
type bashTool struct {
	provider      capability.SandboxProvider
	allowHostBash bool
}

func (t *bashTool) Name() string { return "bash" }
func (t *bashTool) Description() string {
	return "在沙盒中执行 bash 命令。始终使用绝对路径。"
}

func (t *bashTool) Run(ctx context.Context, argsJSON string) (string, error) {
	var args struct {
		Command     string `json:"command"`
		Description string `json:"description"`
	}
	_ = json.Unmarshal([]byte(argsJSON), &args)

	threadID := capability.ThreadIDFrom(ctx)
	sb, id, err := ensureSandbox(t.provider, threadID)
	if err != nil {
		return "", err
	}

	if isLocalSandboxID(id) {
		// host bash 安全开关:本地沙盒默认禁用(对应 is_host_bash_allowed)。
		if !t.allowHostBash {
			return "Error: " + LocalHostBashDisabledMessage, nil
		}
		if err := validateBashCommandPaths(args.Command); err != nil {
			return "Error: " + err.Error(), nil
		}
	}
	output := sb.ExecuteCommand(args.Command)
	return truncateBashOutput(output, 20000), nil
}

// validateBashCommandPaths 对应 validate_local_bash_command_paths 的核心:
// 拒绝 file:// URL、拒绝 ".." 段(token 级,对应 _split_shell_tokens + _DOTDOT_PATH_SEGMENT_PATTERN)、
// 拒绝不在白名单的绝对路径(best-effort)。
func validateBashCommandPaths(command string) error {
	// 阻止 file:// URL(绕过绝对路径正则但可本地文件泄露)。
	if strings.Contains(strings.ToLower(command), "file://") {
		return fmt.Errorf("Unsafe file:// URL in command. Use paths under %s", VirtualPathPrefix)
	}
	// 拒绝 ".." 段(按 shell token 拆分后逐 token 检测,对应 _validate_local_bash_shell_tokens)。
	dotdotRe := regexp.MustCompile(`(?:^|[/\\=])\.\.(?:$|[/\\])`)
	for _, token := range splitShellTokens(command) {
		if dotdotRe.MatchString(token) {
			return fmt.Errorf("Access denied: path traversal detected")
		}
	}
	return nil
}

// splitShellTokens 按空白与 shell 分隔符拆分 token(对应 shlex.shlex 的简化)。
func splitShellTokens(command string) []string {
	return strings.FieldsFunc(command, func(r rune) bool {
		return unicode.IsSpace(r) || strings.ContainsRune(`;"'&|<>()`, r)
	})
}

// truncateBashOutput 中间截断(保留头尾各 50%),对应 _truncate_bash_output。
func truncateBashOutput(output string, maxChars int) string {
	if maxChars == 0 || len(output) <= maxChars {
		return output
	}
	total := len(output)
	markerMax := len(fmt.Sprintf("\n... [middle truncated: %d chars skipped] ...\n", total))
	kept := maxChars - markerMax
	if kept <= 0 {
		return output[:maxChars]
	}
	head := kept / 2
	tail := kept - head
	skipped := total - kept
	marker := fmt.Sprintf("\n... [middle truncated: %d chars skipped] ...\n", skipped)
	return output[:head] + marker + output[len(output)-tail:]
}

// DefaultWriteFileMaxBytes 对应 _WRITE_FILE_CONTENT_MAX_BYTES = 80KB。
const DefaultWriteFileMaxBytes = 80 * 1024

// effectiveWriteFileMaxBytes 运行时读 env(对应 _effective_write_file_max_bytes)。
func effectiveWriteFileMaxBytes() int {
	raw := os.Getenv("DEERFLOW_WRITE_FILE_MAX_BYTES")
	if raw == "" {
		return DefaultWriteFileMaxBytes
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return DefaultWriteFileMaxBytes
	}
	return n
}

// writeFileTool 对应 write_file_tool。
type writeFileTool struct {
	provider capability.SandboxProvider
}

func (t *writeFileTool) Name() string { return "write_file" }
func (t *writeFileTool) Description() string {
	return "写文本文件(默认覆盖,append=True 追加)。单次不超过 80KB。"
}

func (t *writeFileTool) Run(ctx context.Context, argsJSON string) (string, error) {
	var args struct {
		Path    string `json:"path"`
		Content string `json:"content"`
		Append  bool   `json:"append"`
	}
	_ = json.Unmarshal([]byte(argsJSON), &args)

	// 80KB 限制(仅非 append):防 LLM 流式 chunk-gap 超时。
	if !args.Append {
		maxBytes := effectiveWriteFileMaxBytes()
		if maxBytes > 0 && len(args.Content) > maxBytes {
			return fmt.Sprintf("Error: write_file content (%d bytes) exceeds the %d-byte single-call limit. Split the content into smaller pieces: either (a) write the first section now, then use str_replace for further edits, or (b) call write_file again with append=True carrying the next section.", len(args.Content), maxBytes), nil
		}
	}

	if err := rejectPathTraversal(args.Path); err != nil {
		return "Error: " + err.Error(), nil
	}
	threadID := capability.ThreadIDFrom(ctx)
	sb, _, err := ensureSandbox(t.provider, threadID)
	if err != nil {
		return "", err
	}
	if err := sb.WriteFile(args.Path, args.Content, args.Append); err != nil {
		return "Error: Failed to write file '" + args.Path + "': " + err.Error(), nil
	}
	return "OK", nil
}

// readFileTool 对应 read_file_tool。
type readFileTool struct {
	provider capability.SandboxProvider
}

func (t *readFileTool) Name() string        { return "read_file" }
func (t *readFileTool) Description() string { return "读取 UTF-8 文本文件。" }

func (t *readFileTool) Run(ctx context.Context, argsJSON string) (string, error) {
	var args struct {
		Path      string `json:"path"`
		StartLine int    `json:"start_line"`
		EndLine   int    `json:"end_line"`
	}
	_ = json.Unmarshal([]byte(argsJSON), &args)

	if err := rejectPathTraversal(args.Path); err != nil {
		return "Error: " + err.Error(), nil
	}
	sb, _, err := ensureSandbox(t.provider, capability.ThreadIDFrom(ctx))
	if err != nil {
		return "", err
	}
	content, err := sb.ReadFile(args.Path)
	if err != nil {
		return "Error: File not found: " + args.Path, nil
	}
	if content == "" {
		return "(empty)", nil
	}
	if args.StartLine > 0 && args.EndLine > 0 {
		lines := strings.Split(content, "\n")
		if args.StartLine-1 < len(lines) {
			end := args.EndLine
			if end > len(lines) {
				end = len(lines)
			}
			content = strings.Join(lines[args.StartLine-1:end], "\n")
		}
	}
	return truncateHeadOutput(content, 50000), nil
}

// truncateHeadOutput 头截断(保留开头),对应 _truncate_read_file_output。
func truncateHeadOutput(output string, maxChars int) string {
	if maxChars == 0 || len(output) <= maxChars {
		return output
	}
	total := len(output)
	markerMax := len(fmt.Sprintf("\n... [truncated: showing first %d of %d chars. Use start_line/end_line to read a specific range] ...", total, total))
	kept := maxChars - markerMax
	if kept <= 0 {
		return output[:maxChars]
	}
	marker := fmt.Sprintf("\n... [truncated: showing first %d of %d chars. Use start_line/end_line to read a specific range] ...", kept, total)
	return output[:kept] + marker
}

// lsTool 对应 ls_tool。
type lsTool struct {
	provider capability.SandboxProvider
}

func (t *lsTool) Name() string        { return "ls" }
func (t *lsTool) Description() string { return "列出目录内容(最多 2 层)。" }

func (t *lsTool) Run(ctx context.Context, argsJSON string) (string, error) {
	var args struct {
		Path string `json:"path"`
	}
	_ = json.Unmarshal([]byte(argsJSON), &args)
	if err := rejectPathTraversal(args.Path); err != nil {
		return "Error: " + err.Error(), nil
	}
	sb, _, err := ensureSandbox(t.provider, capability.ThreadIDFrom(ctx))
	if err != nil {
		return "", err
	}
	children := sb.ListDir(args.Path, 2)
	if len(children) == 0 {
		return "(empty)", nil
	}
	return truncateHeadOutput(strings.Join(children, "\n"), 20000), nil
}

// globTool 对应 glob_tool。
type globTool struct {
	provider capability.SandboxProvider
}

func (t *globTool) Name() string        { return "glob" }
func (t *globTool) Description() string { return "按 glob 模式匹配路径。" }

func (t *globTool) Run(ctx context.Context, argsJSON string) (string, error) {
	var args struct {
		Path        string `json:"path"`
		Pattern     string `json:"pattern"`
		IncludeDirs bool   `json:"include_dirs"`
		MaxResults  int    `json:"max_results"`
	}
	_ = json.Unmarshal([]byte(argsJSON), &args)
	if args.MaxResults <= 0 {
		args.MaxResults = 200
	}
	if args.MaxResults > 1000 {
		args.MaxResults = 1000
	}
	sb, _, err := ensureSandbox(t.provider, capability.ThreadIDFrom(ctx))
	if err != nil {
		return "", err
	}
	matches, truncated, err := sb.Glob(args.Path, args.Pattern, args.IncludeDirs, args.MaxResults)
	if err != nil {
		return "Error: Directory not found: " + args.Path, nil
	}
	return formatGlobResults(args.Path, matches, truncated), nil
}

func formatGlobResults(root string, matches []string, truncated bool) string {
	if len(matches) == 0 {
		return "No files matched under " + root
	}
	header := fmt.Sprintf("Found %d paths under %s", len(matches), root)
	if truncated {
		header += fmt.Sprintf(" (showing first %d)", len(matches))
	}
	var b strings.Builder
	b.WriteString(header)
	for i, p := range matches {
		b.WriteString(fmt.Sprintf("\n%d. %s", i+1, p))
	}
	if truncated {
		b.WriteString("\nResults truncated. Narrow the path or pattern to see fewer matches.")
	}
	return b.String()
}

// grepTool 对应 grep_tool。
type grepTool struct {
	provider capability.SandboxProvider
}

func (t *grepTool) Name() string        { return "grep" }
func (t *grepTool) Description() string { return "在目录内搜索匹配行。" }

func (t *grepTool) Run(ctx context.Context, argsJSON string) (string, error) {
	var args struct {
		Path          string `json:"path"`
		Pattern       string `json:"pattern"`
		Glob          string `json:"glob"`
		Literal       bool   `json:"literal"`
		CaseSensitive bool   `json:"case_sensitive"`
		MaxResults    int    `json:"max_results"`
	}
	_ = json.Unmarshal([]byte(argsJSON), &args)
	if args.MaxResults <= 0 {
		args.MaxResults = 100
	}
	if args.MaxResults > 500 {
		args.MaxResults = 500
	}
	sb, _, err := ensureSandbox(t.provider, capability.ThreadIDFrom(ctx))
	if err != nil {
		return "", err
	}
	matches, truncated, err := sb.Grep(args.Path, args.Pattern, args.Glob, args.Literal, args.CaseSensitive, args.MaxResults)
	if err != nil {
		return "Error: Directory not found: " + args.Path, nil
	}
	return formatGrepResults(args.Path, matches, truncated), nil
}

func formatGrepResults(root string, matches []capability.GrepMatch, truncated bool) string {
	if len(matches) == 0 {
		return "No matches found under " + root
	}
	header := fmt.Sprintf("Found %d matches under %s", len(matches), root)
	if truncated {
		header += fmt.Sprintf(" (showing first %d)", len(matches))
	}
	var b strings.Builder
	b.WriteString(header)
	for _, m := range matches {
		b.WriteString(fmt.Sprintf("\n%s:%d: %s", m.Path, m.LineNumber, m.Line))
	}
	if truncated {
		b.WriteString("\nResults truncated. Narrow the path or add a glob filter.")
	}
	return b.String()
}

// SandboxTools 返回全部沙盒工具(按名字),供 Agent 注册。
// allowHostBash 对应 sandbox.allow_host_bash(默认 false)。
func SandboxTools(provider capability.SandboxProvider, allowHostBash bool) map[string]capability.Tool {
	return map[string]capability.Tool{
		"bash":       &bashTool{provider: provider, allowHostBash: allowHostBash},
		"write_file": &writeFileTool{provider: provider},
		"read_file":  &readFileTool{provider: provider},
		"ls":         &lsTool{provider: provider},
		"glob":       &globTool{provider: provider},
		"grep":       &grepTool{provider: provider},
	}
}

// 编译期静态断言:确保两层沙盒 + provider 实现契约。
var (
	_ capability.Sandbox           = (*LocalSandbox)(nil)
	_ capability.Sandbox           = (*DockerSandbox)(nil)
	_ capability.SandboxProvider   = (*LocalSandboxProvider)(nil)
	_ capability.SandboxProvider   = (*DockerSandboxProvider)(nil)
	_ capability.SandboxShutdowner = (*LocalSandboxProvider)(nil)
	_ capability.SandboxShutdowner = (*DockerSandboxProvider)(nil)
)
