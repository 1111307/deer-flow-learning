// agent-loop(turn / step 双层循环) + 后台 run 编排。
//
// 对应 deer-flow:
//
//   - runtime/runs/worker.py::run_agent 驱动 graph.astream(后台执行)
//
//   - LangGraph 内部的 super-step 循环
//
//   - turn(外层):一次用户输入 → 完整处理 → 回复。一个 turn 可能跨多个 step。
//     对应「一次 run」;用户中断后通过 checkpoint 恢复 = 下一次 turn 接着跑。
//
//   - step(内层):一次模型调用 → 若有 tool_calls 则执行工具并把结果回填 →
//     再调模型。对应 LangGraph 的一个 super-step(一次 LLM 调用 + 一次工具节点)。
//
// 两层循环各有独立的终止条件:
//   - step 循环:模型不再返回 tool_calls(纯文本)就结束;超过 MaxSteps 报错
//     (对应 recursion_limit=100,防 LLM 死循环)。
//   - turn 循环:由上层(Gateway / IM channel)驱动,用户每发一条消息进入一次 turn。
//
// run_agent 的后台执行(goroutine + context + channel)对应 worker.py 里
// asyncio.create_task + graph.astream + abort_event + 最终状态机。
package harness

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"deerflow-go/capability"
	"deerflow-go/runtime"
)

// Run 处理一个 turn:追加用户消息,然后跑内层 step 循环。
func (a *Agent) Run(ctx context.Context, t *Thread, userInput string) (*TurnResult, error) {
	if err := a.ensureSandbox(t); err != nil {
		return nil, err
	}
	t.Messages = append(t.Messages, capability.Message{Role: "user", Content: userInput})
	return a.runSteps(ctx, t)
}

// Resume 在用户回答了 interrupt 之后继续同一个 turn。
// 对应 deer-flow:挂起后用户回复,下一次 run 从 checkpoint 恢复继续跑。
func (a *Agent) Resume(ctx context.Context, t *Thread, answer string) (*TurnResult, error) {
	t.Messages = append(t.Messages, capability.Message{Role: "user", Content: answer})
	return a.runSteps(ctx, t)
}

// runSteps 是内层 step 循环(对应 LangGraph 的 super-step 循环)。
func (a *Agent) runSteps(ctx context.Context, t *Thread) (*TurnResult, error) {
	for step := 0; step < a.MaxSteps; step++ {
		// 每次迭代前检查 ctx 取消(对应 abort_event / task.cancel 打断 astream)。
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		// 每次模型调用前先检查是否需要压缩上下文(见 compaction.go)。
		if err := a.maybeCompact(ctx, t); err != nil {
			return nil, err
		}

		// 发模型请求前的两个中间件(对应 wrap_model_call 阶段):
		// 1. 悬空调用补偿 —— 补合成 ToolMessage,让「tool_calls 紧跟 tool 结果」配对合法。
		// 2. 循环警告注入 —— 把排队的 loop warning 追加到消息列表末尾(不能插在配对中间)。
		if a.PatchDangling {
			t.Messages = PatchDangling(t.Messages)
		}
		if a.LoopDetector != nil {
			for _, w := range a.LoopDetector.DrainPending() {
				t.Messages = append(t.Messages, capability.Message{Role: "user", Content: w, Name: "loop_warning"})
			}
		}

		resp, err := a.Model.Chat(ctx, capability.ChatRequest{Messages: t.Messages})
		if err != nil {
			return nil, err
		}

		// 模型直接返回 interrupt(供应商层直接请求人工)——挂起。
		if resp.Interrupt != nil {
			t.Messages = append(t.Messages, resp.Message)
			a.notifyStep(t)
			return &TurnResult{Interrupt: resp.Interrupt}, nil
		}

		// 纯文本回复:本轮 step 结束。
		if len(resp.Message.ToolCalls) == 0 {
			t.Messages = append(t.Messages, resp.Message)
			a.notifyStep(t)
			return &TurnResult{Reply: resp.Message.Content}, nil
		}

		// 循环检测(对应 after_model):硬停止则清空 tool_calls,逼模型输出收尾文本。
		if a.LoopDetector != nil {
			if hardStop, msg := a.LoopDetector.AfterModel(resp.Message.ToolCalls); hardStop {
				t.Messages = append(t.Messages, capability.Message{Role: "assistant", Content: msg})
				a.notifyStep(t)
				return &TurnResult{Reply: msg}, nil
			}
		}

		// 有工具调用:先落 assistant 消息,再逐个执行工具并回填 tool 消息。
		t.Messages = append(t.Messages, resp.Message)
		for _, call := range resp.Message.ToolCalls {
			// 把 thread_id 注入 ctx,沙盒工具据此按线程复用沙盒。
			toolCtx := capability.WithThreadID(ctx, t.ID)
			out, interrupt, err := a.runTool(toolCtx, call)
			if err != nil {
				return nil, err
			}
			// 工具链里产生了 interrupt(如 ask_clarification 被拦截)——挂起。
			// 关键(和 deer-flow 一致):ClarificationMiddleware 返回的 Command 会补一条
			// ToolMessage(问题内容),这里用 interrupt.Question 作 tool 消息内容回填,
			// 解决「assistant.tool_calls 悬空」,否则下次模型调用会被 provider 400 拒绝。
			if interrupt != nil {
				t.Messages = append(t.Messages, capability.Message{
					Role:       "tool",
					ToolCallID: call.ID,
					Name:       call.Name,
					Content:    interrupt.Question,
				})
				a.notifyStep(t)
				return &TurnResult{Interrupt: interrupt}, nil
			}
			// 关键:tool 消息必须紧跟对应的 assistant.tool_calls,否则 provider
			// (OpenAI/Anthropic)会拒绝 —— 对应 deer-flow 的 dangling tool call 问题。
			t.Messages = append(t.Messages, capability.Message{
				Role:       "tool",
				ToolCallID: call.ID,
				Name:       call.Name,
				Content:    out,
			})
		}
		a.notifyStep(t)
	}
	return nil, fmt.Errorf("recursion_limit (%d steps) exceeded", a.MaxSteps)
}

// notifyStep 在每个 step 结束后把最新消息快照交给 StepObserver(若配置),
// 供 run_agent 以 stream_mode="values" 发布状态。
func (a *Agent) notifyStep(t *Thread) {
	if a.StepObserver != nil {
		a.StepObserver(t.Messages)
	}
}

// runTool 通过中间件链执行一次工具调用。
func (a *Agent) runTool(ctx context.Context, call capability.ToolCall) (string, *capability.InterruptRequest, error) {
	// 最内层:真正执行工具。
	base := func(ctx context.Context, call capability.ToolCall) (string, *capability.InterruptRequest, error) {
		tool, ok := a.Tools[call.Name]
		if !ok {
			// 对应 deer-flow:未知工具不崩 run,返回一条可读错误让模型自行纠偏。
			return fmt.Sprintf("unknown tool: %s", call.Name), nil, nil
		}
		out, err := tool.Run(ctx, call.Args)
		return out, nil, err
	}
	handler := chain(base, a.Middleware)
	return handler(ctx, call)
}

// ensureSandbox 懒初始化沙盒(对应 deer-flow tools.py::ensure_sandbox_initialized)。
// 第一次用到沙盒工具时才 acquire,之后缓存在 Thread.SandboxID 复用。
func (a *Agent) ensureSandbox(t *Thread) error {
	if a.Sandbox == nil || t.SandboxID != "" {
		return nil
	}
	id, err := a.Sandbox.Acquire(t.ID)
	if err != nil {
		return err
	}
	t.SandboxID = id
	return nil
}

// ---------------------------------------------------------------------------
// run_agent —— 后台执行编排(对应 worker.py::run_agent)
// ---------------------------------------------------------------------------

// RunAgent 在后台执行一个 turn,并把事件流式发布到 StreamBridge。
// 对应 worker.py::run_agent:asyncio.create_task 里驱动 graph.astream,
// abort_event 打断、rollback 回滚 checkpoint、最终状态 success/interrupted/error。
//
// 调用方用 `go RunAgent(...)` 实现后台执行(等价于 asyncio.create_task)。
//
// Go 与 Python 的关键差异:
//   - asyncio.Task → goroutine;task.cancel() → context.CancelFunc。
//   - asyncio.Event → 关闭一次 chan struct{}(abortCh)。
//   - checkpointer 的 pre-run checkpoint 快照 → 浅拷贝 thread.Messages 切片。
//   - graph.astream(stream_mode="values") → StepObserver 每 step 发布消息快照。
func RunAgent(bridge *runtime.StreamBridge, mgr *RunManager, rec *RunRecord, agent *Agent, thread *Thread, userInput string) {
	runID := rec.RunID
	ctx, cancel := context.WithCancel(context.Background())
	rec.cancel = cancel
	// 把 abort_event 绑定到 context:一旦触发立即打断正在跑的 step 循环。
	go func() {
		<-rec.abortCh
		cancel()
	}()

	// 已经(在启动前)被 abort:直接终态,不跑图。
	if rec.Aborted() {
		mgr.SetStatus(runID, RunInterrupted, "")
		bridge.PublishEnd(runID)
		go bridge.CleanupAfter(runID, time.Minute)
		return
	}

	// 1. Mark running。
	mgr.SetStatus(runID, RunRunning, "")

	// 2. 快照 run 前 checkpoint(rollback 时恢复)。
	preRun := append([]capability.Message(nil), thread.Messages...)

	// 3. 发布 metadata(useStream 需要 run_id + thread_id)。
	meta, _ := json.Marshal(map[string]string{"run_id": runID, "thread_id": rec.ThreadID})
	bridge.Publish(runID, "metadata", string(meta))

	// 4. 追加用户消息,驱动 step 循环,每 step 发布 values 快照。
	thread.Messages = append(thread.Messages, capability.Message{Role: "user", Content: userInput})
	prev := agent.StepObserver
	agent.StepObserver = func(msgs []capability.Message) {
		data, err := json.Marshal(msgs)
		if err != nil {
			return
		}
		bridge.Publish(runID, "values", string(data))
	}
	_, runErr := agent.runSteps(ctx, thread)
	agent.StepObserver = prev

	// 5. 最终状态机(对应 worker.py 的第 8 步)。
	switch {
	case rec.Aborted():
		if rec.AbortAction == "rollback" {
			// rollback:回滚到 run 前 checkpoint,状态 error。
			mgr.SetStatus(runID, RunError, "Rolled back by user")
			thread.Messages = preRun
		} else {
			mgr.SetStatus(runID, RunInterrupted, "")
		}
	case runErr != nil:
		mgr.SetStatus(runID, RunError, runErr.Error())
		payload, _ := json.Marshal(map[string]string{"message": runErr.Error(), "name": "error"})
		bridge.Publish(runID, "error", string(payload))
	default:
		mgr.SetStatus(runID, RunSuccess, "")
	}

	// 6. 结束哨兵 + 延迟清理(对应 publish_end + cleanup(delay=60))。
	bridge.PublishEnd(runID)
	go bridge.CleanupAfter(runID, time.Minute)
}
