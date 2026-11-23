package scheduler

import (
	"context"
	"sync"

	"evalreel/internal/store"
)

// Queue 定义任务排队接口；v2.0 使用本地内存通道，v3.0 可替换为 Redis/Asynq 队列。
type Queue interface {
	Push(ctx context.Context, tasks []store.EvalTask) error
	Pop(ctx context.Context) (store.EvalTask, bool, error)
	Close() error
}

// MemoryQueue 是 Queue 的单机内存实现，底层使用带缓冲的 Go channel。
type MemoryQueue struct {
	ch        chan store.EvalTask
	closeOnce sync.Once
}

// NewMemoryQueue 创建指定容量的内存任务队列。
func NewMemoryQueue(capacity int) *MemoryQueue {
	if capacity <= 0 {
		capacity = 1
	}
	return &MemoryQueue{
		ch: make(chan store.EvalTask, capacity),
	}
}

// Push 将一批待执行子任务放入队列，监听上下文取消以防阻塞。
func (q *MemoryQueue) Push(ctx context.Context, tasks []store.EvalTask) error {
	for _, tk := range tasks {
		select {
		case q.ch <- tk:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// Pop 从队列取出一条任务；队列已关闭且排空时返回 false。
func (q *MemoryQueue) Pop(ctx context.Context) (store.EvalTask, bool, error) {
	select {
	case tk, ok := <-q.ch:
		if !ok {
			return store.EvalTask{}, false, nil
		}
		return tk, true, nil
	case <-ctx.Done():
		return store.EvalTask{}, false, ctx.Err()
	}
}

// Close 安全关闭队列通道，支持重复调用不触发 panic。
func (q *MemoryQueue) Close() error {
	q.closeOnce.Do(func() {
		close(q.ch)
	})
	return nil
}
