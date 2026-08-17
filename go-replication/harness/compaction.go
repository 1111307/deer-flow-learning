// context-compaction(长对话压缩)。
//
// 对应 deer-flow:
//   - agents/middlewares/summarization_middleware.py::DeerFlowSummarizationMiddleware
//   - config/summarization_config.py::SummarizationConfig(trigger / keep)
//   - agents/memory/summarization_hook.py::memory_flush_hook(压缩前先刷长期记忆)
//   - agents/memory/message_processing.py(过滤 + correction/reinforcement 检测)
//
// 核心:长任务上下文窗口有限,靠「阈值触发 + 摘要压缩 + 保留最近原文」控制。
// deer-flow 的 trigger/keep 都支持三维度(messages / tokens / fraction)。
//
// 关键生产细节(逐行对应 summarization_middleware.py):
//  1. trigger/keep 三维度:任一 trigger 命中即压缩;keep 决定压缩后保留多少。
//  2. skill 救援:最近加载的 skill 文件(读 /mnt/skills 下的文件)不参与压缩,
//     受 preserve_recent_skill_count / tokens / per_skill 三个预算约束。
//  3. TAG_NOSTREAM:摘要 LLM 调用打上 no-stream 标签,防止其 token 流被
//     messages-tuple 回调捕获后广播成「幻影 AI 消息」泄漏到前端。
//  4. memory_flush_hook:压缩前先把要压掉的消息刷进长期记忆(过滤 + correction/
//     reinforcement 检测),避免关键事实凭空消失。
//  5. 摘要 prompt 保留关键决策 / 产物路径 / 未完成 todo。
package harness

import (
	"context"
	"encoding/json"
	"regexp"
	"sort"
	"strings"

	"deerflow-go/capability"
)

// ---------------------------------------------------------------------------
// 配置(对应 config/summarization_config.py)
// ---------------------------------------------------------------------------

// ContextSizeType 上下文尺寸的三种量纲。
type ContextSizeType string

const (
	ContextFraction ContextSizeType = "fraction"
	ContextTokens   ContextSizeType = "tokens"
	ContextMessages ContextSizeType = "messages"
)

// ContextSize 一次 trigger / keep 的规格(对应 ContextSize pydantic model)。
type ContextSize struct {
	Type  ContextSizeType
	Value float64
}

// SummarizationConfig 摘要配置(对应 SummarizationConfig)。
type SummarizationConfig struct {
	Enabled   bool
	ModelName string
	// Trigger 触发阈值(任一命中即压缩)。nil = 不触发。
	Trigger []ContextSize
	// Keep 压缩后保留多少原文。
	Keep ContextSize
	// TrimTokensToSummarize 压缩前对「待摘要消息」的 token 上限(0 表示不裁剪)。
	TrimTokensToSummarize int
	// SummaryPrompt 自定义摘要 prompt(空 = 用默认 prompt)。
	SummaryPrompt string
	// Skill 救援预算(对应 preserve_recent_skill_*)。
	PreserveRecentSkillCount          int
	PreserveRecentSkillTokens         int
	PreserveRecentSkillTokensPerSkill int
	SkillFileReadToolNames            []string
	SkillsContainerPath               string
}

// DefaultSummarizationConfig 返回 deer-flow 默认配置:
// enabled=false、keep 20 条、skill 救援 count=5 / tokens=25000 / per_skill=5000。
func DefaultSummarizationConfig() SummarizationConfig {
	return SummarizationConfig{
		Enabled:                           false,
		Keep:                              ContextSize{Type: ContextMessages, Value: 20},
		TrimTokensToSummarize:             4000,
		PreserveRecentSkillCount:          5,
		PreserveRecentSkillTokens:         25000,
		PreserveRecentSkillTokensPerSkill: 5000,
		SkillFileReadToolNames:            []string{"read_file", "read", "view", "cat"},
		SkillsContainerPath:               "/mnt/skills",
	}
}

// DefaultSummaryPrompt 默认摘要 prompt:要求保留关键决策 / 产物路径 / 未完成 todo。
// 对应 LangChain 默认 summary prompt + deer-flow 对摘要质量的要求。
const DefaultSummaryPrompt = `Provide a detailed summary of the conversation to date. Preserve:
- key decisions and their rationale
- artifact and file paths produced
- unfinished todos and pending follow-ups

<messages>
{messages}
</messages>`

// ---------------------------------------------------------------------------
// 压缩前钩子(对应 summarization_hook.py::memory_flush_hook + SummarizationEvent)
// ---------------------------------------------------------------------------

// SummarizationEvent 压缩前抛出的上下文(对应 SummarizationEvent)。
type SummarizationEvent struct {
	MessagesToSummarize []capability.Message
	PreservedMessages   []capability.Message
	ThreadID            string
	AgentName           string
	UserID              string
}

// BeforeSummarizationHook 压缩前钩子(对应 BeforeSummarizationHook Protocol)。
type BeforeSummarizationHook func(event *SummarizationEvent)

// MemoryEnqueueFunc 是长期记忆入队回调。由 memory agent 用 MemoryQueue.Add 包装提供,
// 这里只依赖函数签名,避免 compaction 直接 import MemoryQueue(解耦)。
type MemoryEnqueueFunc func(threadID, agentName, userID string, msgs []capability.Message, correction, reinforcement bool)

// MemoryFlushHook 返回 memory_flush_hook 的 Go 形态:压缩前把要压掉的消息
// 过滤后刷进长期记忆队列,并检测 correction / reinforcement 信号。
// 对应 summarization_hook.py::memory_flush_hook:
//  1. 过滤消息(filter_messages_for_memory:只留用户输入 + 无 tool_calls 的最终 AI 回复)。
//  2. 只有 human 与 assistant 消息都存在时才入队。
//  3. correction 检测;reinforcement 仅在「非 correction」时检测。
func MemoryFlushHook(enqueue MemoryEnqueueFunc) BeforeSummarizationHook {
	return func(event *SummarizationEvent) {
		if event.ThreadID == "" {
			return
		}
		filtered := filterMessagesForMemory(event.MessagesToSummarize)
		var hasHuman, hasAssistant bool
		for _, m := range filtered {
			if m.Role == "user" {
				hasHuman = true
			} else if m.Role == "assistant" {
				hasAssistant = true
			}
		}
		if !hasHuman || !hasAssistant {
			return
		}
		correction := detectCorrection(filtered)
		reinforcement := !correction && detectReinforcement(filtered)
		if enqueue != nil {
			enqueue(event.ThreadID, event.AgentName, event.UserID, filtered, correction, reinforcement)
		}
	}
}

// ---------------------------------------------------------------------------
// 消息过滤 + 信号检测(对应 message_processing.py)
// ---------------------------------------------------------------------------

var uploadBlockRe = regexp.MustCompile(`(?i)<uploaded_files>[\s\S]*?</uploaded_files>\n*`)

var correctionPatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)\bthat(?:'s| is) (?:wrong|incorrect)\b`),
	regexp.MustCompile(`(?i)\byou misunderstood\b`),
	regexp.MustCompile(`(?i)\btry again\b`),
	regexp.MustCompile(`(?i)\bredo\b`),
	regexp.MustCompile(`不对`),
	regexp.MustCompile(`你理解错了`),
	regexp.MustCompile(`你理解有误`),
	regexp.MustCompile(`重试`),
	regexp.MustCompile(`重新来`),
	regexp.MustCompile(`换一种`),
	regexp.MustCompile(`改用`),
}

var reinforcementPatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)\byes[,.]?\s+(?:exactly|perfect|that(?:'s| is) (?:right|correct|it))\b`),
	regexp.MustCompile(`(?i)\bperfect(?:[.!?]|$)`),
	regexp.MustCompile(`(?i)\bexactly\s+(?:right|correct)\b`),
	regexp.MustCompile(`(?i)\bthat(?:'s| is)\s+(?:exactly\s+)?(?:right|correct|what i (?:wanted|needed|meant))\b`),
	regexp.MustCompile(`(?i)\bkeep\s+(?:doing\s+)?that\b`),
	regexp.MustCompile(`(?i)\bjust\s+(?:like\s+)?(?:that|this)\b`),
	regexp.MustCompile(`(?i)\bthis is (?:great|helpful)\b(?:[.!?]|$)`),
	regexp.MustCompile(`(?i)\bthis is what i wanted\b(?:[.!?]|$)`),
	regexp.MustCompile(`对[，,]?\s*就是这样(?:[。！？!?.]|$)`),
	regexp.MustCompile(`完全正确(?:[。！？!?.]|$)`),
	regexp.MustCompile(`(?:对[，,]?\s*)?就是这个意思(?:[。！？!?.]|$)`),
	regexp.MustCompile(`正是我想要的(?:[。！？!?.]|$)`),
	regexp.MustCompile(`继续保持(?:[。！？!?.]|$)`),
}

// filterMessagesForMemory 只保留用户输入与「无 tool_calls 的最终 AI 回复」。
// 对应 filter_messages_for_memory:纯上传块的人类消息剥离上传内容;若剥离后为空,
// 则跳过该条并连带跳过下一条 AI(上传确认模板,不进记忆)。
func filterMessagesForMemory(messages []capability.Message) []capability.Message {
	var filtered []capability.Message
	skipNextAI := false
	for _, m := range messages {
		switch m.Role {
		case "user":
			content := m.Content
			if strings.Contains(content, "<uploaded_files>") {
				stripped := strings.TrimSpace(uploadBlockRe.ReplaceAllString(content, ""))
				if stripped == "" {
					skipNextAI = true
					continue
				}
				m.Content = stripped
				filtered = append(filtered, m)
				skipNextAI = false
			} else {
				filtered = append(filtered, m)
				skipNextAI = false
			}
		case "assistant":
			if len(m.ToolCalls) == 0 {
				if skipNextAI {
					skipNextAI = false
					continue
				}
				filtered = append(filtered, m)
			}
		}
	}
	return filtered
}

// recentHumanContents 取最近 6 条 user 消息的文本(对应 messages[-6:] 过滤 human)。
func recentHumanContents(messages []capability.Message) []string {
	start := len(messages) - 6
	if start < 0 {
		start = 0
	}
	var out []string
	for _, m := range messages[start:] {
		if m.Role == "user" {
			if c := strings.TrimSpace(m.Content); c != "" {
				out = append(out, c)
			}
		}
	}
	return out
}

// detectCorrection 检测最近的显式用户纠正(对应 detect_correction)。
func detectCorrection(messages []capability.Message) bool {
	for _, content := range recentHumanContents(messages) {
		for _, re := range correctionPatterns {
			if re.MatchString(content) {
				return true
			}
		}
	}
	return false
}

// detectReinforcement 检测最近的显式正向强化(对应 detect_reinforcement)。
func detectReinforcement(messages []capability.Message) bool {
	for _, content := range recentHumanContents(messages) {
		for _, re := range reinforcementPatterns {
			if re.MatchString(content) {
				return true
			}
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// 压缩主逻辑(对应 _maybe_summarize)
// ---------------------------------------------------------------------------

// maybeCompact 在每次模型调用前检查是否超阈值,超了就压缩。
// 放在 runSteps 的 step 开头调用 —— 对应 SummarizationMiddleware 在
// wrap_model_call / before_model 阶段(真正发模型请求前)执行。
func (a *Agent) maybeCompact(ctx context.Context, t *Thread) error {
	cfg := a.Compaction
	if !cfg.Enabled {
		return nil
	}
	messages := t.Messages
	if len(messages) == 0 {
		return nil
	}

	totalTokens := a.tokens(messages)
	if !a.shouldSummarize(messages, totalTokens) {
		return nil
	}

	cutoff := a.determineCutoffIndex(messages)
	if cutoff <= 0 {
		return nil
	}

	toSummarize, preserved := a.partitionWithSkillRescue(messages, cutoff)
	toSummarize, preserved = a.preserveDynamicContextReminders(toSummarize, preserved)

	a.fireHooks(toSummarize, preserved, t)

	summary, err := a.summarize(ctx, toSummarize)
	if err != nil {
		return err
	}

	// 用一条 name="summary" 的 user 消息替换旧历史,再拼上保留的最近原文。
	// name="summary" 对应 deer-flow _build_new_messages:前端忽略显示,但模型可用作上下文。
	newMsg := capability.Message{
		Role:    "user",
		Content: "Here is a summary of the conversation to date:\n\n" + summary,
		Name:    "summary",
	}
	t.Messages = append([]capability.Message{newMsg}, preserved...)
	return nil
}

func (a *Agent) tokens(msgs []capability.Message) int {
	if a.TokenCounter != nil {
		return a.TokenCounter(msgs)
	}
	return defaultTokenCounter(msgs)
}

// shouldSummarize 判断是否触发压缩(任一 trigger 命中即压缩)。
func (a *Agent) shouldSummarize(messages []capability.Message, totalTokens int) bool {
	for _, tr := range a.Compaction.Trigger {
		if a.triggerMet(tr, len(messages), totalTokens) {
			return true
		}
	}
	return false
}

func (a *Agent) triggerMet(tr ContextSize, msgCount, totalTokens int) bool {
	switch tr.Type {
	case ContextMessages:
		return msgCount >= int(tr.Value)
	case ContextTokens:
		return totalTokens >= int(tr.Value)
	case ContextFraction:
		return float64(totalTokens) >= tr.Value*float64(a.MaxInputTokens)
	default:
		return false
	}
}

// determineCutoffIndex 计算「待摘要/保留」的分界下标(对应 _determine_cutoff_index)。
// 返回的 cutoff 满足:保留 messages[cutoff:],摘要 messages[:cutoff]。
func (a *Agent) determineCutoffIndex(messages []capability.Message) int {
	keep := a.Compaction.Keep
	switch keep.Type {
	case ContextMessages:
		n := int(keep.Value)
		if n <= 0 {
			n = 1
		}
		if n >= len(messages) {
			return 0
		}
		return len(messages) - n
	case ContextTokens, ContextFraction:
		budget := int(keep.Value)
		if keep.Type == ContextFraction {
			budget = int(keep.Value * float64(a.MaxInputTokens))
		}
		if budget <= 0 {
			return 0
		}
		acc := 0
		for i := len(messages) - 1; i >= 0; i-- {
			acc += a.tokens([]capability.Message{messages[i]})
			if acc > budget {
				return i + 1
			}
		}
		return 0
	default:
		n := 20
		if n >= len(messages) {
			return 0
		}
		return len(messages) - n
	}
}

// partitionMessages 基础分区(对应 LangChain _partition_messages):直接按 cutoff 切。
func partitionMessages(messages []capability.Message, cutoff int) ([]capability.Message, []capability.Message) {
	if cutoff <= 0 {
		return nil, messages
	}
	if cutoff >= len(messages) {
		return messages, nil
	}
	return messages[:cutoff], messages[cutoff:]
}

// preserveDynamicContextReminders 把隐藏的动态上下文提醒(当前日期/记忆)从压缩中保留,
// 避免 DynamicContextMiddleware 把 summary 消息误判成首条用户消息。对应
// _preserve_dynamic_context_reminders。这里用 Name=="dynamic_context" 作标记的简化版。
func (a *Agent) preserveDynamicContextReminders(toSummarize, preserved []capability.Message) ([]capability.Message, []capability.Message) {
	var reminders, remaining []capability.Message
	for _, m := range toSummarize {
		if m.Name == "dynamic_context" {
			reminders = append(reminders, m)
		} else {
			remaining = append(remaining, m)
		}
	}
	if len(reminders) == 0 {
		return toSummarize, preserved
	}
	return remaining, append(reminders, preserved...)
}

// fireHooks 触发压缩前钩子(对应 _fire_hooks):逐个调用,单个失败不影响其它。
func (a *Agent) fireHooks(toSummarize, preserved []capability.Message, t *Thread) {
	if len(a.BeforeSummarization) == 0 {
		return
	}
	event := &SummarizationEvent{
		MessagesToSummarize: toSummarize,
		PreservedMessages:   preserved,
		ThreadID:            t.ID,
	}
	for _, hook := range a.BeforeSummarization {
		hook(event)
	}
}

// summarize 生成摘要(对应 _summarize_with)。空消息返回占位;裁剪后为空返回
// "too long to summarize"。TAG_NOSTREAM 语义:Summarizer 是纯函数(不流式),
// 天然不会被 messages-tuple 回调广播成幻影消息 —— 这是 Go 里对 TAG_NOSTREAM 的等价。
func (a *Agent) summarize(ctx context.Context, msgs []capability.Message) (string, error) {
	if len(msgs) == 0 {
		return "No previous conversation history.", nil
	}
	if _, ok := buildSummaryPrompt(msgs, a.Compaction.TrimTokensToSummarize, a.Compaction.SummaryPrompt, a.tokens); !ok {
		return "Previous conversation was too long to summarize.", nil
	}
	if a.Summarizer != nil {
		return a.Summarizer(ctx, msgs)
	}
	return a.defaultSummarizer(ctx, msgs)
}

// buildSummaryPrompt 构造摘要 prompt(对应 _build_summary_prompt):
// 先裁剪(trim_tokens_to_summarize),再格式化成纯文本,填入 prompt 模板。
// 返回 ok=false 表示裁剪后为空。
func buildSummaryPrompt(msgs []capability.Message, trimTokens int, promptTemplate string, tokenFn func([]capability.Message) int) (string, bool) {
	trimmed := trimMessagesForSummary(msgs, trimTokens, tokenFn)
	if len(trimmed) == 0 {
		return "", false
	}
	formatted := formatMessagesBuffer(trimmed)
	if promptTemplate == "" {
		promptTemplate = DefaultSummaryPrompt
	}
	return strings.Replace(promptTemplate, "{messages}", formatted, 1), true
}

// trimMessagesForSummary 从尾部向前保留,直到 token 预算(对应 _trim_messages_for_summary)。
func trimMessagesForSummary(msgs []capability.Message, trimTokens int, tokenFn func([]capability.Message) int) []capability.Message {
	if trimTokens <= 0 {
		return msgs
	}
	acc := 0
	for i := len(msgs) - 1; i >= 0; i-- {
		acc += tokenFn([]capability.Message{msgs[i]})
		if acc > trimTokens {
			return msgs[i+1:]
		}
	}
	return msgs
}

// formatMessagesBuffer 把消息格式化成纯文本(对应 get_buffer_string:避免元数据 token 膨胀)。
func formatMessagesBuffer(msgs []capability.Message) string {
	var b strings.Builder
	for _, m := range msgs {
		b.WriteString(m.Role)
		b.WriteString(": ")
		b.WriteString(m.Content)
		b.WriteString("\n")
	}
	return b.String()
}

// ---------------------------------------------------------------------------
// Skill 救援(对应 _partition_with_skill_rescue / _find_skill_bundles / _select_bundles_to_rescue)
// ---------------------------------------------------------------------------

type skillBundle struct {
	aiIndex          int
	skillToolIndices []int
	skillToolCallIDs map[string]bool
	skillToolTokens  int
	skillKey         string
}

// partitionWithSkillRescue 先基础分区,再把「最近加载的 skill 文件」从摘要侧救到保留侧。
func (a *Agent) partitionWithSkillRescue(messages []capability.Message, cutoff int) ([]capability.Message, []capability.Message) {
	toSummarize, preserved := partitionMessages(messages, cutoff)

	cfg := a.Compaction
	if cfg.PreserveRecentSkillCount == 0 || cfg.PreserveRecentSkillTokens == 0 || len(toSummarize) == 0 {
		return toSummarize, preserved
	}

	skillsRoot := cfg.SkillsContainerPath
	if skillsRoot == "" {
		skillsRoot = "/mnt/skills"
	}
	bundles := findSkillBundles(toSummarize, skillsRoot, cfg.SkillFileReadToolNames, a.tokens)
	if len(bundles) == 0 {
		return toSummarize, preserved
	}
	rescue := selectBundlesToRescue(bundles, cfg.PreserveRecentSkillCount, cfg.PreserveRecentSkillTokens, cfg.PreserveRecentSkillTokensPerSkill)
	if len(rescue) == 0 {
		return toSummarize, preserved
	}

	bundlesByAI := map[int]*skillBundle{}
	rescueToolIndices := map[int]bool{}
	for _, b := range rescue {
		bundlesByAI[b.aiIndex] = b
		for _, idx := range b.skillToolIndices {
			rescueToolIndices[idx] = true
		}
	}

	var rescued, remaining []capability.Message
	for i, msg := range toSummarize {
		if b := bundlesByAI[i]; b != nil && msg.Role == "assistant" {
			var rescuedCalls, remainingCalls []capability.ToolCall
			for _, tc := range msg.ToolCalls {
				if b.skillToolCallIDs[tc.ID] {
					rescuedCalls = append(rescuedCalls, tc)
				} else {
					remainingCalls = append(remainingCalls, tc)
				}
			}
			if len(rescuedCalls) > 0 {
				cloned := msg
				cloned.ToolCalls = rescuedCalls
				cloned.Content = ""
				rescued = append(rescued, cloned)
			}
			if len(remainingCalls) > 0 || msg.Content != "" {
				cloned := msg
				cloned.ToolCalls = remainingCalls
				remaining = append(remaining, cloned)
			}
			continue
		}
		if rescueToolIndices[i] {
			rescued = append(rescued, msg)
			continue
		}
		remaining = append(remaining, msg)
	}
	return remaining, append(rescued, preserved...)
}

// findSkillBundles 定位「读 skill 文件」的 AI 消息 + 配对 tool 消息组。
func findSkillBundles(messages []capability.Message, skillsRoot string, toolNames []string, tokenFn func([]capability.Message) int) []*skillBundle {
	if len(toolNames) == 0 {
		toolNames = []string{"read_file", "read", "view", "cat"}
	}
	nameSet := map[string]bool{}
	for _, n := range toolNames {
		nameSet[n] = true
	}

	var bundles []*skillBundle
	n := len(messages)
	i := 0
	for i < n {
		msg := messages[i]
		if !(msg.Role == "assistant" && len(msg.ToolCalls) > 0) {
			i++
			continue
		}

		skillPathsByID := map[string]string{}
		for _, tc := range msg.ToolCalls {
			if isSkillToolCall(tc, nameSet, skillsRoot) {
				if tc.ID != "" {
					if p := toolCallPath(tc); p != "" {
						skillPathsByID[tc.ID] = p
					}
				}
			}
		}
		if len(skillPathsByID) == 0 {
			i++
			continue
		}

		// 统计配对的 tool 消息(紧跟 AI 消息之后连续的一段 tool 消息)。
		j := i + 1
		for j < n && messages[j].Role == "tool" {
			j++
		}

		tokens := 0
		var skillPaths []string
		var toolIndices []int
		callIDs := map[string]bool{}
		for k := i + 1; k < j; k++ {
			tm := messages[k]
			if tm.Role == "tool" {
				if p, ok := skillPathsByID[tm.ToolCallID]; ok {
					tokens += tokenFn([]capability.Message{tm})
					skillPaths = append(skillPaths, p)
					toolIndices = append(toolIndices, k)
					callIDs[tm.ToolCallID] = true
				}
			}
		}
		if len(toolIndices) == 0 {
			i = j
			continue
		}
		sort.Strings(skillPaths)
		bundles = append(bundles, &skillBundle{
			aiIndex:          i,
			skillToolIndices: toolIndices,
			skillToolCallIDs: callIDs,
			skillToolTokens:  tokens,
			skillKey:         strings.Join(skillPaths, "|"),
		})
		i = j
	}
	return bundles
}

// selectBundlesToRescue 在 count/token 预算内,从最新到最旧挑选要救的 bundle。
func selectBundlesToRescue(bundles []*skillBundle, maxCount, maxTokens, perSkillTokens int) []*skillBundle {
	var selected []*skillBundle
	seenKeys := map[string]bool{}
	total := 0
	kept := 0
	for i := len(bundles) - 1; i >= 0; i-- {
		b := bundles[i]
		if kept >= maxCount {
			break
		}
		if seenKeys[b.skillKey] {
			continue
		}
		if b.skillToolTokens > perSkillTokens {
			continue
		}
		if total+b.skillToolTokens > maxTokens {
			continue
		}
		selected = append(selected, b)
		total += b.skillToolTokens
		kept++
		seenKeys[b.skillKey] = true
	}
	// 反转回原始顺序。
	for l, r := 0, len(selected)-1; l < r; l, r = l+1, r-1 {
		selected[l], selected[r] = selected[r], selected[l]
	}
	return selected
}

// isSkillToolCall 判断 tool_call 是否读取 skills_root 下的文件。
func isSkillToolCall(tc capability.ToolCall, nameSet map[string]bool, skillsRoot string) bool {
	if !nameSet[tc.Name] {
		return false
	}
	p := toolCallPath(tc)
	if p == "" {
		return false
	}
	root := strings.TrimRight(skillsRoot, "/")
	return p == root || strings.HasPrefix(p, root+"/")
}

// toolCallPath 从 tool_call 参数里提取文件路径(对应 _tool_call_path)。
func toolCallPath(tc capability.ToolCall) string {
	args := parseToolArgsAny(tc.Args)
	for _, key := range []string{"path", "file_path", "filepath"} {
		if v, ok := args[key].(string); ok && v != "" {
			return v
		}
	}
	return ""
}

// parseToolArgsAny 把 tool_call 的 JSON 参数解析成 map(宽容:解析失败返回空 map)。
func parseToolArgsAny(argsJSON string) map[string]interface{} {
	out := map[string]interface{}{}
	if argsJSON == "" {
		return out
	}
	_ = json.Unmarshal([]byte(argsJSON), &out)
	return out
}
