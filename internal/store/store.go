package store

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrNotFound 表示查询的总任务、子任务或报告编号不存在。
	ErrNotFound = errors.New("record not found")
	// ErrDuplicateKey 表示触发唯一索引冲突（例如重复提交了相同的幂等键）。
	ErrDuplicateKey = errors.New("duplicate key")
)

// Store 定义评测状态持久化的统一接口；本地单机模式使用 SQLite，分布式服务端模式使用 MySQL。
type Store interface {
	CreateRun(ctx context.Context, run *EvalRun, tasks []EvalTask) error
	FindRunByIdempotencyKey(ctx context.Context, key string) (*EvalRun, error)
	ClaimPendingTasks(ctx context.Context, runID string) ([]EvalTask, error)
	GetTask(ctx context.Context, taskID string) (*EvalTask, error)
	MarkTaskRunning(ctx context.Context, taskID string, retryCount int) error
	UpdateTaskResult(ctx context.Context, taskID string, update TaskResultUpdate) error
	UpdateRunStatus(ctx context.Context, runID string, status RunStatus) error
	ResetFailedTasks(ctx context.Context, runID string) (int64, error)
	TryFinalizeRun(ctx context.Context, runID string, finishedAt time.Time) (bool, error)
	SaveScores(ctx context.Context, scores []EvalScore) error
	GetRun(ctx context.Context, runID string) (*EvalRun, []EvalTask, []EvalScore, error)
	GetRunMeta(ctx context.Context, runID string) (*EvalRun, error)
	ListRuns(ctx context.Context, limit int) ([]EvalRun, error)
	SaveReport(ctx context.Context, report *EvalReport) error
	GetReport(ctx context.Context, runID string) (*EvalReport, error)
	SaveSpans(ctx context.Context, spans []TraceSpan) error
	ListSpansByTraceID(ctx context.Context, traceID string) ([]TraceSpan, error)
	Close() error
}
