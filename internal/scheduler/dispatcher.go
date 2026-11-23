package scheduler

import (
	"context"

	"evalreel/internal/config"
	"evalreel/internal/store"
)

// TaskTypeEval 是分布式任务队列中子评测任务的统一任务类型名称。
const TaskTypeEval = "eval:task"

// EvalTaskPayload 是进入分布式任务队列的轻量载荷，仅携带定位主键以减小 Redis 内存开销。
type EvalTaskPayload struct {
	RunID  string `json:"run_id"`
	TaskID string `json:"task_id"`
}

// QueueStats 汇总分布式任务队列当前的堆积、重试与死信数量。
type QueueStats struct {
	Queue     string `json:"queue"`
	Pending   int    `json:"pending"`
	Active    int    `json:"active"`
	Scheduled int    `json:"scheduled"`
	Retry     int    `json:"retry"`
	Archived  int    `json:"archived"`
	Completed int    `json:"completed"`
}

// Dispatcher 抽象“如何把一场评测拆出来的子任务分发出去执行”。
type Dispatcher interface {
	DispatchRun(ctx context.Context, runID string, tasks []store.EvalTask, cfg config.EvalConfig) error
	Close() error
}
