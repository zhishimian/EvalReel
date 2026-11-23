package store

import (
	"context"
	"errors"
)

// ErrNotFound 表示查询的总任务或报告编号不存在。
var ErrNotFound = errors.New("record not found")

// Store 定义评测状态持久化的统一接口；v2.0 使用本地 SQLite，v3.0 可无缝切换为 MySQL。
type Store interface {
	CreateRun(ctx context.Context, run *EvalRun, tasks []EvalTask) error
	ClaimPendingTasks(ctx context.Context, runID string) ([]EvalTask, error)
	MarkTaskRunning(ctx context.Context, taskID string, retryCount int) error
	UpdateTaskResult(ctx context.Context, taskID string, update TaskResultUpdate) error
	UpdateRunStatus(ctx context.Context, runID string, status RunStatus) error
	SaveScores(ctx context.Context, scores []EvalScore) error
	GetRun(ctx context.Context, runID string) (*EvalRun, []EvalTask, []EvalScore, error)
	ListRuns(ctx context.Context, limit int) ([]EvalRun, error)
	SaveReport(ctx context.Context, report *EvalReport) error
	GetReport(ctx context.Context, runID string) (*EvalReport, error)
	Close() error
}
