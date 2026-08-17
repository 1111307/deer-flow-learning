// Package runtime 是 Gateway/SSE 流式层 —— 对应 deer-flow runtime/stream_bridge/memory.py。
//
// StreamBridge 是 per-run 的内存事件日志:worker 往里 publish 事件,SSE 消费者
// subscribe 后重放 + 实时接收。与 deer-flow MemoryStreamBridge 一致的核心设计:
//   - Last-Event-ID 重放:断线重连的客户端从上次事件 id 之后继续,不丢事件。
//   - 有界缓冲:每个 run 只保留最近 N 个事件(queue_maxsize=256),防止长 run 内存无限增长。
//     溢出时丢弃最旧事件并推进 start_offset。
//   - 心跳:无事件时也周期性推 heartbeat(heartbeat_interval=15s),保证 SSE keep-alive。
//   - ended 哨兵:publish_end 后,订阅者收完缓冲事件再收到 __end__ 哨兵后关闭。
//   - cleanup 延迟删除:晚到订阅者有时间 drain 剩余事件。
//
// Go 与 Python 的关键差异:
//   - Python 用 asyncio.Condition + async iterator(消费者「拉取」共享缓冲);
//     Go 用「每个订阅者一个 goroutine + 阻塞 channel 发送」实现同样的拉取语义 ——
//     订阅者以自己的节奏阻塞地从共享缓冲读,事件不会因消费者慢而丢失(与 memory.py 一致,
//     它直接从 stream.events[local_index] 读,不存在 per-subscriber 队列)。
//   - asyncio.Event / Condition.notify_all → 每个订阅者独立的 cap=1 wake channel。
//   - asyncio.sleep(delay) → time.AfterFunc(delay, ...)。
package runtime

import (
	"fmt"
	"sync"
	"time"
)

// DefaultHeartbeatInterval 无事件时的默认心跳间隔(对应 heartbeat_interval=15.0)。
const DefaultHeartbeatInterval = 15 * time.Second

// DefaultQueueMaxsize 每个 run 保留的事件上限(对应 queue_maxsize=256)。
const DefaultQueueMaxsize = 256

// StreamEvent 一个流事件。对应 stream_bridge/base.py::StreamEvent。
type StreamEvent struct {
	// ID 单调递增的事件 id(SSE 的 id: 字段,支持 Last-Event-ID 重连)。
	ID string
	// Event SSE 事件名,如 "metadata" / "values" / "error" / "__heartbeat__" / "__end__"。
	Event string
	// Data 事件负载(JSON 字符串)。
	Data string
}

// 哨兵事件(对应 base.py 的 HEARTBEAT_SENTINEL / END_SENTINEL)。
var (
	HeartbeatSentinel = StreamEvent{Event: "__heartbeat__"}
	EndSentinel       = StreamEvent{Event: "__end__"}
)

// subscriber 是一个 SSE 消费者的订阅状态。
type subscriber struct {
	out    chan StreamEvent // 消费者拉取的通道
	offset int              // 绝对事件偏移(基于全局 startOffset)
	wake   chan struct{}    // cap=1,唤醒信号
}

// runStream 一个 run 的事件日志(对应 _RunStream)。
type runStream struct {
	events      []StreamEvent
	startOffset int  // 已丢弃的最旧事件绝对偏移(缓冲滑动窗口的起点)
	ended       bool // publish_end 已调用
	seq         int  // 事件序号(0 起)
	subs        map[int]*subscriber
	nextSub     int
}

// StreamBridge 是 per-run 内存事件日志。对应 MemoryStreamBridge。
type StreamBridge struct {
	mu      sync.Mutex
	streams map[string]*runStream
	maxsize int
}

// NewStreamBridge 构造一个每 run 最多缓冲 maxsize 个事件的桥。
func NewStreamBridge(maxsize int) *StreamBridge {
	if maxsize <= 0 {
		maxsize = DefaultQueueMaxsize
	}
	return &StreamBridge{streams: map[string]*runStream{}, maxsize: maxsize}
}

// get 惰性创建 run 的事件日志(对应 _get_or_create_stream)。调用方必须持锁。
func (b *StreamBridge) get(runID string) *runStream {
	s, ok := b.streams[runID]
	if !ok {
		s = &runStream{subs: map[int]*subscriber{}}
		b.streams[runID] = s
	}
	return s
}

// notifySub 非阻塞唤醒一个订阅者(对应 Condition.notify_all 的等价)。
func notifySub(wake chan struct{}) {
	select {
	case wake <- struct{}{}:
	default:
	}
}

// Publish 发布一个事件并唤醒所有订阅者。对应 MemoryStreamBridge.publish。
func (b *StreamBridge) Publish(runID, event, data string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s := b.get(runID)
	id := fmt.Sprintf("%d-%d", time.Now().UnixMilli(), s.seq)
	s.seq++
	s.events = append(s.events, StreamEvent{ID: id, Event: event, Data: data})
	if len(s.events) > b.maxsize {
		overflow := len(s.events) - b.maxsize
		s.events = s.events[overflow:]
		s.startOffset += overflow
	}
	for _, sub := range s.subs {
		notifySub(sub.wake)
	}
}

// PublishEnd 标记 run 结束(对应 publish_end):置 ended 哨兵并唤醒订阅者。
func (b *StreamBridge) PublishEnd(runID string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s := b.get(runID)
	s.ended = true
	for _, sub := range s.subs {
		notifySub(sub.wake)
	}
}

// Subscribe 订阅一个 run(默认心跳间隔):先重放 lastEventID 之后的缓冲事件,
// 再实时接收新事件。对应 MemoryStreamBridge.subscribe(last_event_id=...)。
func (b *StreamBridge) Subscribe(runID, lastEventID string) <-chan StreamEvent {
	return b.SubscribeHeartbeat(runID, lastEventID, DefaultHeartbeatInterval)
}

// SubscribeHeartbeat 订阅并指定心跳间隔。
func (b *StreamBridge) SubscribeHeartbeat(runID, lastEventID string, heartbeat time.Duration) <-chan StreamEvent {
	if heartbeat <= 0 {
		heartbeat = DefaultHeartbeatInterval
	}
	b.mu.Lock()
	s := b.get(runID)

	// 解析起始偏移(对应 _resolve_start_offset)。
	start := s.startOffset
	if lastEventID != "" {
		found := false
		for i, e := range s.events {
			if e.ID == lastEventID {
				start = s.startOffset + i + 1
				found = true
				break
			}
		}
		if !found && len(s.events) > 0 {
			// last_event_id 不在保留缓冲里:从最早保留的事件重放(memory.py 记警告)。
			start = s.startOffset
		}
	}

	sub := &subscriber{
		out:    make(chan StreamEvent, b.maxsize),
		offset: start,
		wake:   make(chan struct{}, 1),
	}
	id := s.nextSub
	s.nextSub++
	s.subs[id] = sub
	b.mu.Unlock()

	go b.deliver(s, id, sub, heartbeat)
	return sub.out
}

// deliver 是订阅者 goroutine:以消费者节奏从共享缓冲拉取事件,补心跳,收尾 __end__。
func (b *StreamBridge) deliver(s *runStream, id int, sub *subscriber, heartbeat time.Duration) {
	// 结束时注销订阅并关闭通道。
	defer func() {
		b.mu.Lock()
		delete(s.subs, id)
		b.mu.Unlock()
		close(sub.out)
	}()

	ticker := time.NewTicker(heartbeat)
	defer ticker.Stop()

	for {
		if b.drainAvailable(s, sub) {
			return // 已发出 __end__ 哨兵,结束
		}
		select {
		case <-sub.wake:
			// 有新事件或 ended,回到 drainAvailable 重查。
		case <-ticker.C:
			// 心跳:无事件时 keep-alive(通道满则丢弃,消费者可重连重放)。
			select {
			case sub.out <- HeartbeatSentinel:
			default:
			}
		}
	}
}

// drainAvailable 阻塞地把当前缓冲事件逐个交给消费者(拉取语义,不丢事件),
// 若已 ended 且缓冲耗尽则发送 __end__ 哨兵并返回 true。
func (b *StreamBridge) drainAvailable(s *runStream, sub *subscriber) bool {
	for {
		b.mu.Lock()
		// 落后于保留缓冲(缓冲滑窗已越过其 offset):重置到最早保留事件。
		if sub.offset < s.startOffset {
			sub.offset = s.startOffset
		}
		if sub.offset-s.startOffset < len(s.events) {
			e := s.events[sub.offset-s.startOffset]
			sub.offset++
			b.mu.Unlock()
			sub.out <- e // 阻塞:消费者拉取
			continue
		}
		ended := s.ended
		b.mu.Unlock()
		if ended {
			sub.out <- EndSentinel
			return true
		}
		return false
	}
}

// Cleanup 删除一个 run 的缓冲(对应 cleanup 的立即删除)。
func (b *StreamBridge) Cleanup(runID string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.streams, runID)
}

// CleanupAfter 延迟删除一个 run 的缓冲(对应 cleanup(delay=...):给晚到订阅者 drain 时间)。
func (b *StreamBridge) CleanupAfter(runID string, delay time.Duration) {
	if delay <= 0 {
		b.Cleanup(runID)
		return
	}
	time.AfterFunc(delay, func() { b.Cleanup(runID) })
}

// Close 释放所有 run 的资源(对应 close:清空 streams)。
func (b *StreamBridge) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.streams = map[string]*runStream{}
}
