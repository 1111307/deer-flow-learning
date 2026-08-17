// 能力三件套之二:Provider(提供者工厂)。
//
// 对应 deer-flow:
//   - sandbox/sandbox_provider.py::SandboxProvider(ABC) + get_sandbox_provider()
//   - models/factory.py 里的 resolve_class(config.sandbox.use / model.use) 反射实例化
//
// deer-flow 用字符串配置(如 `use: deerflow.sandbox.local:LocalSandboxProvider`)
// + 反射(resolve_class)在运行时选择实现。Go 的惯用替代是「注册表 + 工厂函数」:
// 编译期就把 name -> factory 登记好,运行时按配置字符串查表实例化。
// 两者都实现了同一件事:上层只认名字,不 import 具体实现(解耦 + 可测试)。
//
// resolve_class("module:Class") 在 Go 里退化为两层:
//  1. 注册表查表(NewSandboxProvider):provider 的 init() 同时登记短名("local")和
//     完整 class path("deerflow.sandbox.local:LocalSandboxProvider"),所以传入
//     配置字符串或短名都能命中 —— 等价于 Python 的 import + getattr + issubclass 校验。
//  2. 单例缓存(GetSandboxProvider):等价于 _default_sandbox_provider 全局单例。
package capability

import (
	"fmt"
	"sync"
)

// SandboxProviderFactory 是「无参构造一个 provider」的工厂签名。
type SandboxProviderFactory func() SandboxProvider

// ModelProviderFactory 是模型供应商的工厂签名。
type ModelProviderFactory func() ModelProvider

var (
	sandboxMu        sync.RWMutex
	sandboxProviders = map[string]SandboxProviderFactory{}
	modelMu          sync.RWMutex
	modelProviders   = map[string]ModelProviderFactory{}

	// 单例缓存(对应 sandbox_provider.py 的 _default_sandbox_provider)。
	// 用 provider 锁保护,保证 get/reset/shutdown/set 的线程安全。
	singletonMu        sync.Mutex
	defaultSandboxProv SandboxProvider
	defaultSandboxName = "local"
)

// RegisterSandboxProvider 注册沙盒提供者。各 provider 包在 init() 里调用。
// 允许同名重复注册(后注册覆盖先注册),便于测试替换实现。
func RegisterSandboxProvider(name string, f SandboxProviderFactory) {
	if name == "" || f == nil {
		panic("capability: RegisterSandboxProvider with empty name or nil factory")
	}
	sandboxMu.Lock()
	defer sandboxMu.Unlock()
	sandboxProviders[name] = f
}

// NewSandboxProvider 按名字实例化沙盒提供者(对应 resolve_class)。
// name 既可以是短名("local" / "docker"),也可以是完整 class path
// ("deerflow.sandbox.local:LocalSandboxProvider")。
func NewSandboxProvider(name string) (SandboxProvider, error) {
	sandboxMu.RLock()
	defer sandboxMu.RUnlock()
	f, ok := sandboxProviders[name]
	if !ok {
		return nil, fmt.Errorf("unknown sandbox provider %q", name)
	}
	return f(), nil
}

// RegisterModelProvider 注册模型供应商。
func RegisterModelProvider(name string, f ModelProviderFactory) {
	if name == "" || f == nil {
		panic("capability: RegisterModelProvider with empty name or nil factory")
	}
	modelMu.Lock()
	defer modelMu.Unlock()
	modelProviders[name] = f
}

// NewModelProvider 按名字实例化模型供应商。
func NewModelProvider(name string) (ModelProvider, error) {
	modelMu.RLock()
	defer modelMu.RUnlock()
	f, ok := modelProviders[name]
	if !ok {
		return nil, fmt.Errorf("unknown model provider %q", name)
	}
	return f(), nil
}

// SetSandboxProviderName 设置 get_sandbox_provider() 单例使用的 provider 名。
// 对应 deer-flow 里 config.sandbox.use 决定实例化哪个 provider。
// 若当前已有单例,则先 Reset(清空缓存),下次 Get 按新名字重建。
func SetSandboxProviderName(name string) {
	singletonMu.Lock()
	defer singletonMu.Unlock()
	defaultSandboxName = name
	defaultSandboxProv = nil
}

// GetSandboxProvider 返回沙盒 provider 单例(懒初始化)。
// 对应 sandbox_provider.py::get_sandbox_provider()。首次调用按 defaultSandboxName
// 查注册表实例化并缓存;后续调用返回同一实例。
func GetSandboxProvider() SandboxProvider {
	singletonMu.Lock()
	defer singletonMu.Unlock()
	if defaultSandboxProv == nil {
		p, err := NewSandboxProvider(defaultSandboxName)
		if err != nil {
			panic(fmt.Sprintf("capability: failed to instantiate sandbox provider: %v", err))
		}
		defaultSandboxProv = p
	}
	return defaultSandboxProv
}

// ResetSandboxProvider 清空单例缓存(不清空底层 provider 的资源)。
// 对应 reset_sandbox_provider():先调用 provider.reset() 清空跨实例状态,再置空单例。
// 下次 GetSandboxProvider() 会创建新实例。
func ResetSandboxProvider() {
	singletonMu.Lock()
	defer singletonMu.Unlock()
	if defaultSandboxProv != nil {
		defaultSandboxProv.Reset()
		defaultSandboxProv = nil
	}
}

// ShutdownSandboxProvider 优雅关闭并清空单例。
// 对应 shutdown_sandbox_provider():若 provider 实现了 Shutdown(hasattr 检查),
// 先 shutdown(释放所有沙盒),再置空单例。
func ShutdownSandboxProvider() {
	singletonMu.Lock()
	p := defaultSandboxProv
	defaultSandboxProv = nil
	singletonMu.Unlock()

	if p != nil {
		if s, ok := p.(SandboxShutdowner); ok {
			s.Shutdown()
		}
	}
}

// SetSandboxProvider 注入一个自定义 provider(测试/替换用)。
// 对应 set_sandbox_provider()。
func SetSandboxProvider(p SandboxProvider) {
	singletonMu.Lock()
	defer singletonMu.Unlock()
	defaultSandboxProv = p
}
