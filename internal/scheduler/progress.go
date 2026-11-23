package scheduler

import (
	"sync"
	"time"

	"evalreel/internal/store"
)

// ProgressEvent 描述单次进度更新事件，供 SSE 接口实时推送给前端或命令行。
type ProgressEvent struct {
	RunID      string          `json:"run_id"`
	Total      int             `json:"total"`
	Done       int             `json:"done"`
	Failed     int             `json:"failed"`
	Status     store.RunStatus `json:"status"`
	LastTaskID string          `json:"last_task_id,omitempty"`
	Timestamp  time.Time       `json:"timestamp"`
}

// ProgressBroker 负责将调度进度非阻塞地广播给所有订阅同一场评测的客户端。
type ProgressBroker struct {
	mu   sync.RWMutex
	subs map[string]map[chan ProgressEvent]struct{}
}

// NewProgressBroker 创建一个并发安全的进度订阅广播器。
func NewProgressBroker() *ProgressBroker {
	return &ProgressBroker{
		subs: make(map[string]map[chan ProgressEvent]struct{}),
	}
}

// Subscribe 订阅指定 runID 的实时进度事件，返回只读事件通道与取消订阅清理函数。
func (b *ProgressBroker) Subscribe(runID string) (<-chan ProgressEvent, func()) {
	ch := make(chan ProgressEvent, 64)

	b.mu.Lock()
	if b.subs[runID] == nil {
		b.subs[runID] = make(map[chan ProgressEvent]struct{})
	}
	b.subs[runID][ch] = struct{}{}
	b.mu.Unlock()

	var once sync.Once
	cancel := func() {
		once.Do(func() {
			b.mu.Lock()
			if set := b.subs[runID]; set != nil {
				delete(set, ch)
				if len(set) == 0 {
					delete(b.subs, runID)
				}
			}
			close(ch)
			b.mu.Unlock()
		})
	}
	return ch, cancel
}

// Publish 广播一条进度事件；若某个订阅端消费太慢则直接跳过，绝不拖慢后台协程池。
func (b *ProgressBroker) Publish(ev ProgressEvent) {
	if b == nil {
		return
	}
	if ev.Timestamp.IsZero() {
		ev.Timestamp = time.Now().UTC()
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	set := b.subs[ev.RunID]
	for ch := range set {
		select {
		case ch <- ev:
		default:
		}
	}
}
