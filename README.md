# DeerFlow 源码学习笔记

> 基于 `bytedance/deer-flow` 2.0 后端代码整理的 Agent 岗位面试学习笔记。
> 从源码精读出发,把项目里体现的工程设计转化成可以讲清楚的知识点。

## 入口

- **[learn.md](learn.md)** —— 学习指南总览(速查/总览性质)
- **[go-developer-guide.md](go-developer-guide.md)** —— Go 开发者速查:Python 并发/异步模型对照

## 逐行精读笔记(01-14)

按模块编号的逐行代码精读,打开真实源码、一行一行讲为什么这么写:

- [01-state-and-middleware.md](01-state-and-middleware.md) —— Agent 状态管理 + Middleware 责任链
- [02-loop-detection-and-safety.md](02-loop-detection-and-safety.md) —— 循环检测 + 安全兜底
- [03-dangling-tool-call.md](03-dangling-tool-call.md) —— Dangling Tool Call 消息补偿
- [04-deferred-tool-binding.md](04-deferred-tool-binding.md) —— 延迟工具绑定
- [05-subagent-delegation.md](05-subagent-delegation.md) —— Sub-Agent 多智能体委派
- [06-sandbox-execution.md](06-sandbox-execution.md) —— Sandbox 执行环境与安全
- [07-context-engineering.md](07-context-engineering.md) —— Context Engineering:摘要与预算控制
- [08-long-term-memory.md](08-long-term-memory.md) —— 长期记忆系统
- [09-mcp-integration.md](09-mcp-integration.md) —— MCP 协议集成
- [10-skills-system.md](10-skills-system.md) —— Skills 系统
- [11-model-abstraction.md](11-model-abstraction.md) —— 模型抽象层
- [12-reliability-practices.md](12-reliability-practices.md) —— 工程可靠性实践
- [13-sandbox-docker.md](13-sandbox-docker.md) —— 沙箱 Docker 深入
- [14-sandbox-tools-and-local.md](14-sandbox-tools-and-local.md) —— 沙箱工具与 LocalSandbox

## 面试问题链(qa-01 ~ qa-15)

按「大厂面试追问链」组织,每条问题链从基础问题层层追问到实现细节、设计权衡、异常边界:

- [qa-01-agent-loop-and-state.md](qa-01-agent-loop-and-state.md) —— Agent 主循环与状态管理
- [qa-02-safety-middlewares.md](qa-02-safety-middlewares.md) —— 安全中间件
- [qa-03-tools-deferred-binding.md](qa-03-tools-deferred-binding.md) —— 工具体系与延迟绑定
- [qa-04-subagents.md](qa-04-subagents.md) —— Sub-Agent 委派
- [qa-05-sandbox.md](qa-05-sandbox.md) —— Sandbox 执行环境与安全
- [qa-06-context-engineering.md](qa-06-context-engineering.md) —— Context Engineering
- [qa-07-memory.md](qa-07-memory.md) —— 长期记忆
- [qa-08-mcp.md](qa-08-mcp.md) —— MCP 协议集成
- [qa-09-skills.md](qa-09-skills.md) —— Skills 系统
- [qa-10-models.md](qa-10-models.md) —— 模型抽象层
- [qa-11-gateway-runtime.md](qa-11-gateway-runtime.md) —— Gateway 与运行时
- [qa-12-persistence-migrations.md](qa-12-persistence-migrations.md) —— 持久化与 Schema 迁移
- [qa-13-channels.md](qa-13-channels.md) —— IM Channels 多平台接入
- [qa-14-reliability.md](qa-14-reliability.md) —— 工程可靠性实践
- [qa-15-observability-guardrails-misc.md](qa-15-observability-guardrails-misc.md) —— 可观测/护栏/上传/社区工具/反射/配置

## 概念补充

- [concept-ai-evaluation.md](concept-ai-evaluation.md) —— AI/Agent 评测体系

## Go 复现专题

- **[go-replication/](go-replication/)** —— 用 Go 不砍生产级细节地复现 deer-flow 16 个核心能力
  (capability-seam / agent-loop / HITL / compaction / sandbox / state-reducer / loop-detection /
  dangling-tool-call / deferred-tool / long-term-memory / sub-agent / mcp / skills /
  model-abstraction / stream-bridge / guardrail),含逐行源码对照、时序图、链路图。
  详见 [go-replication/README.md](go-replication/README.md)。
