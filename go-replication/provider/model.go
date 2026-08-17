// Package provider 是「AI 供应商」角色 —— 本文件是模型抽象层,对应 deer-flow:
//
//   - models/factory.py::create_chat_model(+ _deep_merge_dicts / _vllm_disable_chat_template_kwargs /
//     _enable_stream_usage_by_default / _apply_stream_chunk_timeout_default)
//   - models/patched_deepseek.py::PatchedChatDeepSeek(reasoning_content 保留)
//   - models/assistant_payload_replay.py(provider 补丁统一模式)
//   - models/vllm_provider.py(VllmChatModel 的 reasoning 保留 + chat_template_kwargs 归一化)
//   - models/openai_codex_provider.py(CodexChatModel 的 reasoning_effort 方言)
//
// 核心设计:
//   - create_chat_model 把一个「thinking_enabled bool」翻译成各厂商的「开/关思考」方言。
//     关键:thinking 的开启/关闭是**结构驱动**的(factory.py:126-157 读 when_thinking_enabled /
//     thinking / when_thinking_disabled 的 shape 判断方言),而不是 switch(use);只有
//     Codex/MindIE/stream_usage/chunk_timeout 这些补丁按 use 字符串 / class 判断。
//   - 三个 LangChain 默认值补丁:stream_usage(OpenAI 兼容网关无自定义 base_url 时默认开)、
//     stream_chunk_timeout(OpenAI 兼容默认 240s,其它 provider 删 key)、deep_merge(递归合并)。
//   - provider 补丁统一模式:LangChain 把 reasoning_content/reasoning 存在 additional_kwargs,
//     但序列化请求 payload 时丢弃;assistant_payload_replay.py 提供共享的 assistant 匹配
//     (签名 + ordinal 回退),deepseek / vLLM 各自决定回填哪个字段。
//
// Go 与 Python 的关键差异:
//   - resolve_class(字符串反射)换成「注册表 + 工厂函数」:map[use]ModelFactory,编译期登记、
//     运行时查表。未知 use 返回 error(等价 resolve_class 的 ImportError/AttributeError)。
//   - dict.update 是浅合并、_deep_merge_dicts 是深合并且不改变入参,Go 里分别用
//     shallowUpdate(原地)与 deepMergeDicts(返回新 map)区分,语义一一对应。
//   - PyYAML/.get 链在中间为 null 时会抛 AttributeError;Go 里用安全的 nestedGet 导航,
//     把「非 map」当「缺失」,不 panic(这是更防御性的 Go 惯用形态)。
package provider

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
)

// ---------------------------------------------------------------------------
// 配置类型(对应 config.yaml 的 models 条目 + AppConfig)
// ---------------------------------------------------------------------------

// ModelConfig 是单个模型声明(factory.py 里 model_config 的 Go 形态)。
//
// Settings 是「除被 exclude 字段外的所有透传字段」—— 对应 factory.py:111-125 的
// model_dump(exclude_none=True, exclude={use,name,display_name,description,
// supports_thinking,supports_reasoning_effort,when_thinking_enabled,
// when_thinking_disabled,thinking,supports_vision})。这些 exclude 字段在 Go 里
// 提升为显式 struct 字段,天然排除出 Settings。
type ModelConfig struct {
	Name                    string
	Use                     string // 如 "langchain_openai:ChatOpenAI" / "deerflow.models.claude_provider:ClaudeChatModel"
	DisplayName             string
	Description             string
	SupportsThinking        bool
	SupportsReasoningEffort bool
	SupportsVision          bool
	// WhenThinkingEnabled 是「开启思考时整体注入的 settings」(浅合并,结构决定方言)。
	WhenThinkingEnabled map[string]any
	// WhenThinkingDisabled 是「关闭思考时的 settings」,优先级最高。
	WhenThinkingDisabled map[string]any
	// Thinking 是 when_thinking_enabled["thinking"] 的快捷方式(Anthropic 方言的 type/budget)。
	Thinking map[string]any
	// Settings 是所有其它透传字段(model/max_tokens/base_url/extra_body/...)。
	Settings map[string]any
}

// AppConfig 是应用配置(models 列表 + 技能审查模型名)。
type AppConfig struct {
	Models              []ModelConfig
	ModerationModelName string
}

// GetModelConfig 按名字取模型配置,找不到返回 nil(对应 factory.py:107-108)。
func (c *AppConfig) GetModelConfig(name string) *ModelConfig {
	for i := range c.Models {
		if c.Models[i].Name == name {
			return &c.Models[i]
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// 注册表 + 工厂(resolve_class 的 Go 替代)
// ---------------------------------------------------------------------------

// 已知 use 路径(与 deer-flow 配置里的 use 字符串逐字一致)。
const (
	UseChatOpenAI      = "langchain_openai:ChatOpenAI"
	UseChatAnthropic   = "langchain_anthropic:ChatAnthropic"
	UseChatDeepSeek    = "langchain_deepseek:ChatDeepSeek"
	UsePatchedDeepSeek = "deerflow.models.patched_deepseek:PatchedChatDeepSeek"
	UseClaudeChatModel = "deerflow.models.claude_provider:ClaudeChatModel"
	UseVllmChatModel   = "deerflow.models.vllm_provider:VllmChatModel"
	UseCodexChatModel  = "deerflow.models.openai_codex_provider:CodexChatModel"
	UseMindIEChatModel = "deerflow.models.mindie_provider:MindIEChatModel"
)

// ModelClass 是 resolve_class 返回的「模型类」在 Go 里的形态:静态的方言 + 供应商默认值。
type ModelClass struct {
	// Dialect 是供应商方言标识(openai/anthropic/vllm/codex/mindie/deepseek)。
	Dialect string
	// Defaults 是供应商级的构造默认值(如 claude 的 enable_prompt_caching=true、
	// codex 的 reasoning_effort="medium"),配置里的 Settings 会浅覆盖它们。
	Defaults map[string]any
	// HasStreamUsageField 对应 `"stream_usage" in model_class.model_fields`:
	// ChatOpenAI 系模型有该字段,stream_usage 默认值补丁才生效。
	HasStreamUsageField bool
}

// ModelFactory 是「无参构造一个模型类」的工厂签名。
type ModelFactory func() *ModelClass

var (
	modelRegistryMu sync.RWMutex
	modelRegistry   = map[string]ModelFactory{}
)

// RegisterModelFactory 登记一个模型供应商。等价于 resolve_class 里可 import 的类。
func RegisterModelFactory(use string, f ModelFactory) {
	modelRegistryMu.Lock()
	defer modelRegistryMu.Unlock()
	modelRegistry[use] = f
}

// resolveModelFactory 按 use 字符串查表(对应 resolve_class 反射实例化)。
func resolveModelFactory(use string) (*ModelClass, error) {
	modelRegistryMu.RLock()
	defer modelRegistryMu.RUnlock()
	f, ok := modelRegistry[use]
	if !ok {
		return nil, fmt.Errorf("unknown model provider %q (resolve_class failed)", use)
	}
	return f(), nil
}

func init() {
	RegisterModelFactory(UseChatOpenAI, func() *ModelClass {
		return &ModelClass{Dialect: "openai", HasStreamUsageField: true}
	})
	RegisterModelFactory(UseChatAnthropic, func() *ModelClass {
		return &ModelClass{Dialect: "anthropic"}
	})
	RegisterModelFactory(UseChatDeepSeek, func() *ModelClass {
		return &ModelClass{Dialect: "deepseek", HasStreamUsageField: true}
	})
	RegisterModelFactory(UsePatchedDeepSeek, func() *ModelClass {
		return &ModelClass{Dialect: "deepseek", HasStreamUsageField: true}
	})
	RegisterModelFactory(UseClaudeChatModel, func() *ModelClass {
		return &ModelClass{Dialect: "anthropic", Defaults: map[string]any{"enable_prompt_caching": true}}
	})
	RegisterModelFactory(UseVllmChatModel, func() *ModelClass {
		return &ModelClass{Dialect: "vllm", HasStreamUsageField: true}
	})
	RegisterModelFactory(UseCodexChatModel, func() *ModelClass {
		return &ModelClass{Dialect: "codex", Defaults: map[string]any{"reasoning_effort": "medium"}}
	})
	RegisterModelFactory(UseMindIEChatModel, func() *ModelClass {
		return &ModelClass{Dialect: "mindie", HasStreamUsageField: true}
	})
}

// ThinkingDialect 把 use 字符串映射成方言标识,用于「能力声明 + 方言归一化」叙事
// 以及 Codex/MindIE/stream 补丁的 class 判断(README §14)。
func ThinkingDialect(use string) string {
	switch use {
	case UseChatAnthropic, UseClaudeChatModel:
		return "anthropic"
	case UseVllmChatModel:
		return "vllm"
	case UseCodexChatModel:
		return "codex"
	case UseMindIEChatModel:
		return "mindie"
	case UseChatDeepSeek, UsePatchedDeepSeek:
		return "deepseek"
	default:
		return "openai" // OpenAI 兼容(deepseek/doubao/moonshot...)
	}
}

// ---------------------------------------------------------------------------
// create_chat_model 的输出 + 工厂
// ---------------------------------------------------------------------------

// ChatModel 是 create_chat_model 解析出的「模型实例描述符」。
// 对应 Python 里 `model_class(**kwargs, **model_settings_from_config)` 的产物:
// 方言 + 最终构造 settings(能力声明也一并保留,方便上层判断)。
type ChatModel struct {
	Name                    string
	Use                     string
	Dialect                 string
	Settings                map[string]any // 最终构造 settings(已合并 kwargs 透传)
	SupportsThinking        bool
	SupportsReasoningEffort bool
	SupportsVision          bool
}

// ---------------------------------------------------------------------------
// factory.py 的字典 helper
// ---------------------------------------------------------------------------

// deepMergeDicts 递归合并两个 dict,不改变入参(factory.py:13-21::_deep_merge_dicts)。
func deepMergeDicts(base, override map[string]any) map[string]any {
	merged := map[string]any{}
	for k, v := range base {
		merged[k] = v
	}
	for k, v := range override {
		if vDict, ok := v.(map[string]any); ok {
			if existing, ok2 := merged[k].(map[string]any); ok2 {
				merged[k] = deepMergeDicts(existing, vDict)
				continue
			}
		}
		merged[k] = v
	}
	return merged
}

// shallowUpdate 对应 dict.update:顶层 key 整体替换(不递归),原地改 dst。
func shallowUpdate(dst, src map[string]any) {
	for k, v := range src {
		dst[k] = v
	}
}

// compactClone 对应 model_dump(exclude_none=True):浅拷贝并丢弃 nil 值。
func compactClone(src map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range src {
		if v == nil {
			continue
		}
		out[k] = v
	}
	return out
}

// nestedGet 安全导航嵌套 map,中间非 map/nil 时视为缺失(Python 的 .get 链在此会抛 AttributeError)。
func nestedGet(m map[string]any, keys ...string) any {
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

func nestedGetString(m map[string]any, keys ...string) string {
	s, _ := nestedGet(m, keys...).(string)
	return s
}

func toMap(v any) map[string]any {
	m, _ := v.(map[string]any)
	return m
}

// vllmDisableChatTemplateKwargs 构建 vLLM 关闭思考的 payload(factory.py:24-31)。
func vllmDisableChatTemplateKwargs(chatTemplateKwargs map[string]any) map[string]any {
	disable := map[string]any{}
	if chatTemplateKwargs == nil {
		return disable
	}
	if _, ok := chatTemplateKwargs["thinking"]; ok {
		disable["thinking"] = false
	}
	if _, ok := chatTemplateKwargs["enable_thinking"]; ok {
		disable["enable_thinking"] = false
	}
	return disable
}

// defaultStreamChunkTimeoutSeconds 是 OpenAI 兼容流式响应的 chunk 间隔预算
// (factory.py:58 的 _DEFAULT_STREAM_CHUNK_TIMEOUT_SECONDS)。
const defaultStreamChunkTimeoutSeconds = 240.0

// enableStreamUsageByDefault 为 OpenAI 兼容模型启用 stream_usage
// (factory.py:34-47::_enable_stream_usage_by_default)。
func enableStreamUsageByDefault(modelUsePath string, settings map[string]any) {
	if modelUsePath != UseChatOpenAI {
		return
	}
	if _, ok := settings["stream_usage"]; ok {
		return
	}
	if _, ok := settings["base_url"]; ok {
		settings["stream_usage"] = true
		return
	}
	if _, ok := settings["openai_api_base"]; ok {
		settings["stream_usage"] = true
	}
}

// applyStreamChunkTimeoutDefault 注入/剔除 stream_chunk_timeout
// (factory.py:61-79::_apply_stream_chunk_timeout_default)。
func applyStreamChunkTimeoutDefault(modelUsePath string, settings map[string]any) {
	if modelUsePath != UseChatOpenAI {
		delete(settings, "stream_chunk_timeout")
		return
	}
	if _, ok := settings["stream_chunk_timeout"]; ok {
		return
	}
	settings["stream_chunk_timeout"] = defaultStreamChunkTimeoutSeconds
}

// CreateChatModel 从配置创建一个 chat model 描述符(factory.py:82-204)。
//
//   - name 为空时取 config.models[0].name(factory.py:105-106)。
//   - kwargs 是额外透传(对应 **kwargs),最终并入 Settings。
//   - attach_tracing 不在本文件范围(build_tracing_callbacks 属于 tracing/factory.py)。
func CreateChatModel(name string, thinkingEnabled bool, cfg *AppConfig, kwargs map[string]any) (*ChatModel, error) {
	if cfg == nil {
		cfg = &AppConfig{}
	}
	if name == "" {
		if len(cfg.Models) == 0 {
			return nil, fmt.Errorf("no models configured")
		}
		name = cfg.Models[0].Name
	}
	modelConfig := cfg.GetModelConfig(name)
	if modelConfig == nil {
		return nil, fmt.Errorf("Model %s not found in config", name)
	}

	class, err := resolveModelFactory(modelConfig.Use)
	if err != nil {
		return nil, err
	}

	// model_dump(exclude_none=True, exclude={...}) → 供应商默认值 + 配置透传字段。
	settings := compactClone(class.Defaults)
	shallowUpdate(settings, compactClone(modelConfig.Settings))

	// 计算 effective_wte:合并 `thinking` 快捷字段(factory.py:126-132)。
	hasThinkingSettings := modelConfig.WhenThinkingEnabled != nil || modelConfig.Thinking != nil
	effectiveWTE := map[string]any{}
	if modelConfig.WhenThinkingEnabled != nil {
		for k, v := range modelConfig.WhenThinkingEnabled {
			effectiveWTE[k] = v
		}
	}
	if modelConfig.Thinking != nil {
		mergedThinking := deepMergeDicts(toMap(effectiveWTE["thinking"]), modelConfig.Thinking)
		effectiveWTE["thinking"] = mergedThinking
	}

	// thinking_enabled 归一化(factory.py:133-157)。
	if thinkingEnabled && hasThinkingSettings {
		if !modelConfig.SupportsThinking {
			return nil, fmt.Errorf("Model %s does not support thinking. Set `supports_thinking` to true in the `config.yaml` to enable thinking.", name)
		}
		if len(effectiveWTE) > 0 {
			shallowUpdate(settings, effectiveWTE)
		}
	}
	if !thinkingEnabled {
		switch {
		case modelConfig.WhenThinkingDisabled != nil:
			// 用户提供的关闭设置优先级最高(factory.py:139-141)。
			shallowUpdate(settings, modelConfig.WhenThinkingDisabled)
		case hasThinkingSettings && nestedGetString(effectiveWTE, "extra_body", "thinking", "type") != "":
			// OpenAI 兼容网关:thinking 嵌套在 extra_body 下(factory.py:142-148)。
			settings["extra_body"] = deepMergeDicts(
				toMap(settings["extra_body"]),
				map[string]any{"thinking": map[string]any{"type": "disabled"}},
			)
			settings["reasoning_effort"] = "minimal"
		case hasThinkingSettings && len(vllmDisableChatTemplateKwargs(toMap(nestedGet(effectiveWTE, "extra_body", "chat_template_kwargs")))) > 0:
			// vLLM 用 chat template kwargs 开关思考(factory.py:149-154)。
			disable := vllmDisableChatTemplateKwargs(toMap(nestedGet(effectiveWTE, "extra_body", "chat_template_kwargs")))
			settings["extra_body"] = deepMergeDicts(
				toMap(settings["extra_body"]),
				map[string]any{"chat_template_kwargs": disable},
			)
		case hasThinkingSettings && nestedGetString(effectiveWTE, "thinking", "type") != "":
			// 原生 langchain_anthropic:thinking 是直接构造参数(factory.py:155-157)。
			settings["thinking"] = map[string]any{"type": "disabled"}
		}
	}

	// 不支持 reasoning_effort 的模型,两边都删(factory.py:158-160)。
	if !modelConfig.SupportsReasoningEffort {
		delete(kwargs, "reasoning_effort")
		delete(settings, "reasoning_effort")
	}

	enableStreamUsageByDefault(modelConfig.Use, settings)
	applyStreamChunkTimeoutDefault(modelConfig.Use, settings)

	// Codex Responses API:thinking 映射到 reasoning_effort(factory.py:165-179)。
	if class.Dialect == "codex" {
		delete(settings, "max_tokens") // Codex endpoint 拒绝 max_tokens/max_output_tokens
		explicitEffort, _ := kwargs["reasoning_effort"].(string)
		delete(kwargs, "reasoning_effort")
		switch {
		case !thinkingEnabled:
			settings["reasoning_effort"] = "none"
		case explicitEffort != "" && (explicitEffort == "low" || explicitEffort == "medium" || explicitEffort == "high" || explicitEffort == "xhigh"):
			settings["reasoning_effort"] = explicitEffort
		default:
			if _, ok := settings["reasoning_effort"]; !ok {
				settings["reasoning_effort"] = "medium"
			}
		}
	}

	// MindIE:强制保守重试默认值(factory.py:181-185)。
	if class.Dialect == "mindie" {
		if _, ok := settings["max_retries"]; !ok {
			settings["max_retries"] = 1
		}
	}

	// stream_usage 通用默认值:有该字段的模型默认开(factory.py:187-194)。
	if _, inSettings := settings["stream_usage"]; !inSettings {
		if _, inKwargs := kwargs["stream_usage"]; !inKwargs {
			if class.HasStreamUsageField {
				settings["stream_usage"] = true
			}
		}
	}

	// model_class(**kwargs, **model_settings_from_config):kwargs 透传并入(前面已保证无 key 冲突)。
	for k, v := range kwargs {
		settings[k] = v
	}

	return &ChatModel{
		Name:                    name,
		Use:                     modelConfig.Use,
		Dialect:                 class.Dialect,
		Settings:                settings,
		SupportsThinking:        modelConfig.SupportsThinking,
		SupportsReasoningEffort: modelConfig.SupportsReasoningEffort,
		SupportsVision:          modelConfig.SupportsVision,
	}, nil
}

// ---------------------------------------------------------------------------
// provider 补丁统一模式:reasoning_content / reasoning 保留
// ---------------------------------------------------------------------------

// PayloadMessage 是序列化后的请求消息(role/content/tool_calls 等),对应 langchain
// 序列化 payload 里的 dict。用 type alias 方便与 map[string]any 互转。
type PayloadMessage = map[string]any

// ChatMessage 是 provider 层消息表示,额外携带 additional_kwargs(LangChain 存了但
// 序列化请求 payload 时丢弃的字段,如 reasoning_content / reasoning)。
type ChatMessage struct {
	Role       string // system / user / assistant / tool
	Content    string
	ToolCalls  []map[string]any // 对应 langchain tool_calls(每个含 id/name/args)
	ToolCallID string
	Additional map[string]any // additional_kwargs
}

// RestoreFunc 把 provider 专属字段回填到一条 payload assistant 消息上。
type RestoreFunc func(payload PayloadMessage, orig ChatMessage)

// messageSignature 是 assistant 消息的签名:(稳定化 content, "|" 连接的 tool_call_ids)。
// 对应 assistant_payload_replay.py::_signature 返回的 tuple。
type messageSignature struct {
	content string
	toolIDs string
}

// RestoreAssistantPayloads 把 provider 专属字段回填到序列化后的 assistant 消息上
// (assistant_payload_replay.py::restore_assistant_payloads)。
//
// 两条路径:长度一致时按位置 zip;长度不一致(序列化丢弃/重排了消息)时按
// 「签名精确匹配 + ordinal 回退」把 assistant payload 对回原始 AIMessage。
func RestoreAssistantPayloads(payloadMsgs []PayloadMessage, originals []ChatMessage, restore RestoreFunc) {
	if len(payloadMsgs) == len(originals) {
		for i := range payloadMsgs {
			if payloadMsgs[i]["role"] == "assistant" && originals[i].Role == "assistant" {
				restore(payloadMsgs[i], originals[i])
			}
		}
		return
	}

	aiMessages := make([]ChatMessage, 0)
	for _, m := range originals {
		if m.Role == "assistant" {
			aiMessages = append(aiMessages, m)
		}
	}
	assistantPayloads := make([]PayloadMessage, 0)
	for _, p := range payloadMsgs {
		if p["role"] == "assistant" {
			assistantPayloads = append(assistantPayloads, p)
		}
	}

	used := map[int]bool{}
	for ordinal, payloadMsg := range assistantPayloads {
		if aiMsg, ok := matchAIMessage(payloadMsg, aiMessages, used, ordinal); ok {
			restore(payloadMsg, aiMsg)
		}
	}
}

// matchAIMessage 对应 assistant_payload_replay.py::_match_ai_message。
func matchAIMessage(payloadMsg PayloadMessage, aiMessages []ChatMessage, used map[int]bool, fallbackOrdinal int) (ChatMessage, bool) {
	if sig := assistantSignature(payloadMsg); sig.content != "" || sig.toolIDs != "" {
		var matches []int
		for i, ai := range aiMessages {
			if !used[i] && aiSignature(ai) == sig {
				matches = append(matches, i)
			}
		}
		if len(matches) == 1 {
			used[matches[0]] = true
			return aiMessages[matches[0]], true
		}
	}

	if idx, ok := nextUnusedIndexAtOrAfter(len(aiMessages), used, fallbackOrdinal); ok {
		used[idx] = true
		return aiMessages[idx], true
	}
	return ChatMessage{}, false
}

// nextUnusedIndexAtOrAfter 对应 _next_unused_index_at_or_after:从 start 向后找未用的 index,
// 不回头(被丢弃的消息可能对应更早的 payload 条目)。
func nextUnusedIndexAtOrAfter(count int, used map[int]bool, start int) (int, bool) {
	if count == 0 || start >= count {
		return 0, false
	}
	for i := start; i < count; i++ {
		if !used[i] {
			return i, true
		}
	}
	return 0, false
}

func assistantSignature(p PayloadMessage) messageSignature {
	return signature(p["content"], toolCallIDs(p["tool_calls"]))
}

func aiSignature(ai ChatMessage) messageSignature {
	toolCalls := ai.ToolCalls
	if len(toolCalls) == 0 {
		if raw, ok := ai.Additional["tool_calls"].([]map[string]any); ok {
			toolCalls = raw
		}
	}
	return signature(ai.Content, toolCallIDs(toolCalls))
}

// signature 对应 _signature:content 为空且无 tool_call_ids 时返回零值(表示「无签名」)。
func signature(content any, toolCallIDs []string) messageSignature {
	if isEmptyContent(content) && len(toolCallIDs) == 0 {
		return messageSignature{}
	}
	return messageSignature{content: stableRepr(content), toolIDs: strings.Join(toolCallIDs, "|")}
}

func isEmptyContent(v any) bool {
	if v == nil {
		return true
	}
	if s, ok := v.(string); ok {
		return s == ""
	}
	return false
}

func toolCallIDs(toolCalls any) []string {
	list, ok := toolCalls.([]map[string]any)
	if !ok {
		if l2, ok2 := toolCalls.([]any); ok2 {
			// 容错:payload 里 tool_calls 常以 []any 出现。
			ids := make([]string, 0, len(l2))
			for _, item := range l2 {
				if m, ok3 := item.(map[string]any); ok3 {
					if id, ok4 := m["id"].(string); ok4 && id != "" {
						ids = append(ids, id)
					}
				}
			}
			return ids
		}
		return nil
	}
	ids := make([]string, 0, len(list))
	for _, tc := range list {
		if id, ok := tc["id"].(string); ok && id != "" {
			ids = append(ids, id)
		}
	}
	return ids
}

// stableRepr 对应 _stable_repr:json.dumps(sort_keys=True, ensure_ascii=False),
// 失败回退 repr。Go 的 json.Marshal 本身对 map key 排序且不转义非 ASCII。
func stableRepr(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf("%v", v)
	}
	return string(b)
}

// RestoreReasoningContent 对应 restore_reasoning_content:回填 additional_kwargs.reasoning_content。
func RestoreReasoningContent(payload PayloadMessage, orig ChatMessage) {
	restoreAdditionalKwargsField(payload, orig, "reasoning_content")
}

// RestoreReasoningField 对应 vllm 的 _restore_reasoning_field:优先 reasoning,回退 reasoning_content。
func RestoreReasoningField(payload PayloadMessage, orig ChatMessage) {
	reasoning, ok := orig.Additional["reasoning"]
	if !ok || reasoning == nil {
		reasoning = orig.Additional["reasoning_content"]
	}
	if reasoning != nil {
		payload["reasoning"] = reasoning
	}
}

// restoreAdditionalKwargsField 对应 restore_additional_kwargs_field。
func restoreAdditionalKwargsField(payload PayloadMessage, orig ChatMessage, fieldName string) {
	if v, ok := orig.Additional[fieldName]; ok && v != nil {
		payload[fieldName] = v
	}
}

// DeepSeekSecrets 对应 PatchedChatDeepSeek.lc_secrets。
var DeepSeekSecrets = map[string]string{
	"api_key":        "DEEPSEEK_API_KEY",
	"openai_api_key": "DEEPSEEK_API_KEY",
}

// DeepSeekBuildPayload 复现 patched_deepseek.py::_get_request_payload 的补丁行为:
// 在父类产出的 base payload 上,把 additional_kwargs.reasoning_content 回填到
// 序列化后的 assistant 消息(思考模式多轮对话要求每个 assistant 消息都带 reasoning_content)。
func DeepSeekBuildPayload(basePayload PayloadMessage, originalMessages []ChatMessage) PayloadMessage {
	msgs := payloadMessages(basePayload["messages"])
	RestoreAssistantPayloads(msgs, originalMessages, RestoreReasoningContent)
	basePayload["messages"] = msgs
	return basePayload
}

// NormalizeVllmChatTemplateKwargs 把 legacy 的 extra_body.chat_template_kwargs.thinking
// 归一化成 vLLM 0.19.0 的 enable_thinking(vllm_provider.py::_normalize_vllm_chat_template_kwargs)。
func NormalizeVllmChatTemplateKwargs(payload PayloadMessage) {
	extraBody, ok := payload["extra_body"].(map[string]any)
	if !ok {
		return
	}
	ctk, ok := extraBody["chat_template_kwargs"].(map[string]any)
	if !ok {
		return
	}
	thinking, ok := ctk["thinking"]
	if !ok {
		return
	}
	normalized := map[string]any{}
	for k, v := range ctk {
		normalized[k] = v
	}
	if _, exists := normalized["enable_thinking"]; !exists {
		normalized["enable_thinking"] = thinking
	}
	delete(normalized, "thinking")
	extraBody["chat_template_kwargs"] = normalized
}

// ReasoningToText 把 vLLM payload 里的 reasoning 尽力抽取成可读文本
// (vllm_provider.py::_reasoning_to_text)。
func ReasoningToText(reasoning any) string {
	switch v := reasoning.(type) {
	case string:
		return v
	case []any:
		var parts []string
		for _, item := range v {
			if s := ReasoningToText(item); s != "" {
				parts = append(parts, s)
			}
		}
		return strings.Join(parts, "")
	case map[string]any:
		for _, key := range []string{"text", "content", "reasoning"} {
			value := v[key]
			if s, ok := value.(string); ok {
				return s
			}
			if value != nil {
				if s := ReasoningToText(value); s != "" {
					return s
				}
			}
		}
		return stableRepr(v)
	default:
		return stableRepr(v)
	}
}

// VllmBuildPayload 复现 vllm_provider.py::VllmChatModel._get_request_payload:
// 先归一化 chat_template_kwargs,再按「长度一致 zip / 不一致按顺序 zip assistant」把
// reasoning(reasoning → reasoning_content 回退)回填到 payload 的 assistant 消息。
func VllmBuildPayload(basePayload PayloadMessage, originalMessages []ChatMessage) PayloadMessage {
	NormalizeVllmChatTemplateKwargs(basePayload)
	payloadMsgs := payloadMessages(basePayload["messages"])

	if len(payloadMsgs) == len(originalMessages) {
		for i := range payloadMsgs {
			if payloadMsgs[i]["role"] == "assistant" && originalMessages[i].Role == "assistant" {
				RestoreReasoningField(payloadMsgs[i], originalMessages[i])
			}
		}
	} else {
		aiMessages := make([]ChatMessage, 0)
		for _, m := range originalMessages {
			if m.Role == "assistant" {
				aiMessages = append(aiMessages, m)
			}
		}
		assistantPayloads := make([]PayloadMessage, 0)
		for _, p := range payloadMsgs {
			if p["role"] == "assistant" {
				assistantPayloads = append(assistantPayloads, p)
			}
		}
		n := len(assistantPayloads)
		if len(aiMessages) < n {
			n = len(aiMessages)
		}
		for i := 0; i < n; i++ {
			RestoreReasoningField(assistantPayloads[i], aiMessages[i])
		}
	}

	basePayload["messages"] = payloadMsgs
	return basePayload
}

// payloadMessages 把 payload["messages"] 归一化成 []PayloadMessage(容错 []any)。
func payloadMessages(v any) []PayloadMessage {
	switch msgs := v.(type) {
	case []PayloadMessage:
		return msgs
	case []any:
		out := make([]PayloadMessage, 0, len(msgs))
		for _, m := range msgs {
			if pm, ok := m.(map[string]any); ok {
				out = append(out, pm)
			}
		}
		return out
	default:
		return nil
	}
}
