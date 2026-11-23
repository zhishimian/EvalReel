package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"evalreel/internal/config"
	"evalreel/internal/store"

	"github.com/hibiken/asynq"
)

// ArchivedTaskInfo 描述 Asynq 死信队列（Archived）中的单条失败任务详情。
type ArchivedTaskInfo struct {
	ID           string    `json:"id"`
	Queue        string    `json:"queue"`
	Type         string    `json:"type"`
	Payload      string    `json:"payload"`
	LastErr      string    `json:"last_err"`
	Retried      int       `json:"retried"`
	MaxRetry     int       `json:"max_retry"`
	LastFailedAt time.Time `json:"last_failed_at"`
}

// AsynqDispatcher 使用 Redis 支撑的 Asynq 队列投递子任务并提供队列状态巡检。
type AsynqDispatcher struct {
	client    *asynq.Client
	inspector *asynq.Inspector
}

// NewAsynqDispatcher 创建基于 Redis 连接参数的 Asynq 分发器与队列检查器。
func NewAsynqDispatcher(addr, password string, db int) *AsynqDispatcher {
	opt := asynq.RedisClientOpt{
		Addr:     addr,
		Password: password,
		DB:       db,
	}
	return &AsynqDispatcher{
		client:    asynq.NewClient(opt),
		inspector: asynq.NewInspector(opt),
	}
}

// DispatchRun 将子任务逐一写入 Redis 队列，带上 TaskID 去重与配置好的最大重试次数和超时限制。
func (d *AsynqDispatcher) DispatchRun(ctx context.Context, runID string, tasks []store.EvalTask, cfg config.EvalConfig) error {
	timeout := time.Duration(cfg.TaskTimeoutSec) * time.Second
	if timeout <= 0 {
		timeout = 120 * time.Second
	}

	for _, tk := range tasks {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		payload, err := json.Marshal(EvalTaskPayload{
			RunID:  runID,
			TaskID: tk.ID,
		})
		if err != nil {
			return fmt.Errorf("marshal task payload %s: %w", tk.ID, err)
		}

		task := asynq.NewTask(TaskTypeEval, payload)
		_, err = d.client.EnqueueContext(
			ctx,
			task,
			asynq.TaskID(tk.ID),
			asynq.MaxRetry(cfg.MaxRetries),
			asynq.Timeout(timeout),
			asynq.Retention(24*time.Hour),
		)
		if err != nil {
			if errors.Is(err, asynq.ErrTaskIDConflict) || errors.Is(err, asynq.ErrDuplicateTask) {
				continue
			}
			return fmt.Errorf("enqueue task %s: %w", tk.ID, err)
		}
	}
	return nil
}

// GetQueueStats 查询默认队列当前的排队、处理中、重试和死信数量。
func (d *AsynqDispatcher) GetQueueStats(_ context.Context) (*QueueStats, error) {
	info, err := d.inspector.GetQueueInfo("default")
	if err != nil {
		if errors.Is(err, asynq.ErrQueueNotFound) {
			return &QueueStats{Queue: "default"}, nil
		}
		return nil, fmt.Errorf("get queue info: %w", err)
	}
	return &QueueStats{
		Queue:     info.Queue,
		Pending:   info.Pending,
		Active:    info.Active,
		Scheduled: info.Scheduled,
		Retry:     info.Retry,
		Archived:  info.Archived,
		Completed: info.Completed,
	}, nil
}

// RedispatchTasks 先清理旧的同编号保留任务（已归档或已完成记录），再重新投递待执行子任务。
func (d *AsynqDispatcher) RedispatchTasks(ctx context.Context, runID string, tasks []store.EvalTask, cfg config.EvalConfig) error {
	for _, tk := range tasks {
		_ = d.inspector.DeleteTask("default", tk.ID)
	}
	return d.DispatchRun(ctx, runID, tasks, cfg)
}

// ListArchivedTasks 列出 default 队列中进入死信（Archived）状态的失败任务。
func (d *AsynqDispatcher) ListArchivedTasks(_ context.Context, limit int) ([]ArchivedTaskInfo, error) {
	if limit <= 0 {
		limit = 50
	}
	list, err := d.inspector.ListArchivedTasks("default", asynq.PageSize(limit), asynq.Page(1))
	if err != nil {
		if errors.Is(err, asynq.ErrQueueNotFound) {
			return []ArchivedTaskInfo{}, nil
		}
		return nil, fmt.Errorf("list archived tasks: %w", err)
	}
	out := make([]ArchivedTaskInfo, 0, len(list))
	for _, item := range list {
		out = append(out, ArchivedTaskInfo{
			ID:           item.ID,
			Queue:        item.Queue,
			Type:         item.Type,
			Payload:      string(item.Payload),
			LastErr:      item.LastErr,
			Retried:      item.Retried,
			MaxRetry:     item.MaxRetry,
			LastFailedAt: item.LastFailedAt,
		})
	}
	return out, nil
}

// RetryArchivedTasks 将指定编号或全部 Asynq 死信队列中的任务重新激活回待执行队列。
func (d *AsynqDispatcher) RetryArchivedTasks(_ context.Context, taskIDs []string, all bool) (int, error) {
	if all {
		n, err := d.inspector.RunAllArchivedTasks("default")
		if err != nil {
			if errors.Is(err, asynq.ErrQueueNotFound) {
				return 0, nil
			}
			return 0, fmt.Errorf("retry all archived tasks: %w", err)
		}
		return n, nil
	}
	var count int
	for _, id := range taskIDs {
		if id == "" {
			continue
		}
		if err := d.inspector.RunTask("default", id); err == nil {
			count++
		}
	}
	return count, nil
}

// Close 关闭底层 Redis 队列客户端与巡检连接。
func (d *AsynqDispatcher) Close() error {
	var firstErr error
	if d.inspector != nil {
		if err := d.inspector.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if d.client != nil {
		if err := d.client.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
