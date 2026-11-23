package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"evalreel/internal/store"
)

// RetryableError 表示遇到 429 限流、5xx 或网络超时等可自动重试的临时错误。
type RetryableError struct {
	RetryAfter time.Duration
	Err        error
}

func (e *RetryableError) Error() string {
	if e.Err == nil {
		return "retryable error"
	}
	return e.Err.Error()
}

func (e *RetryableError) Unwrap() error {
	return e.Err
}

// NewRetryableError 包装一个允许重试的错误，并可选指定服务端要求等待的 RetryAfter 时长。
func NewRetryableError(err error, retryAfter time.Duration) error {
	return &RetryableError{
		RetryAfter: retryAfter,
		Err:        err,
	}
}

// IsRetryable 检查错误是否属于可重试类型，并返回建议等待时间。
func IsRetryable(err error) (time.Duration, bool) {
	var re *RetryableError
	if errors.As(err, &re) {
		return re.RetryAfter, true
	}
	return 0, false
}

// TaskExecutor 定义单条子任务的执行与批卷回调，解耦调度器与具体 Runner/Scorer。
type TaskExecutor func(ctx context.Context, task store.EvalTask) (store.TaskResultUpdate, []store.EvalScore, error)

// Config 配置协程池并发数、单任务超时、最大重试次数与初始退避等待时长。
type Config struct {
	Concurrency int
	MaxRetries  int
	TaskTimeout time.Duration
	BaseBackoff time.Duration
}

// DefaultConfig 返回面向大模型评测的默认调度参数。
func DefaultConfig() Config {
	return Config{
		Concurrency: 10,
		MaxRetries:  2,
		TaskTimeout: 120 * time.Second,
		BaseBackoff: 1 * time.Second,
	}
}

// Scheduler 负责并发拉起工人协程、限流、指数退避重试、写库落盘和进度广播。
type Scheduler struct {
	cfg     Config
	store   store.Store
	limiter Limiter
	broker  *ProgressBroker
	queue   Queue // 若为空则在每次 Run 时按任务数创建一个内存队列
}

// New 创建一个调度器实例。
func New(cfg Config, st store.Store, lim Limiter, broker *ProgressBroker) *Scheduler {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 1
	}
	if cfg.MaxRetries < 0 {
		cfg.MaxRetries = 0
	}
	if cfg.TaskTimeout <= 0 {
		cfg.TaskTimeout = 120 * time.Second
	}
	if cfg.BaseBackoff <= 0 {
		cfg.BaseBackoff = 1 * time.Second
	}
	return &Scheduler{
		cfg:     cfg,
		store:   st,
		limiter: lim,
		broker:  broker,
	}
}

// SetQueue 允许注入自定义分布式队列（例如 v3.0 替换成 Redis/Asynq 队列）。
func (s *Scheduler) SetQueue(q Queue) {
	s.queue = q
}

// Run 从数据库恢复或拉取未完成的子任务并并发跑完，支持断点续跑和中途优雅退出。
func (s *Scheduler) Run(ctx context.Context, runID string, exec TaskExecutor) error {
	if exec == nil {
		return errors.New("task executor is nil")
	}

	run, _, _, err := s.store.GetRun(ctx, runID)
	if err != nil {
		return fmt.Errorf("load run %s: %w", runID, err)
	}

	tasks, err := s.store.ClaimPendingTasks(ctx, runID)
	if err != nil {
		return fmt.Errorf("claim pending tasks: %w", err)
	}

	var doneCount atomic.Int64
	var failedCount atomic.Int64
	doneCount.Store(int64(run.DoneTasks))
	failedCount.Store(int64(run.FailedTasks))

	if len(tasks) == 0 {
		dbCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.store.UpdateRunStatus(dbCtx, runID, store.RunCompleted)
		s.publishProgress(ProgressEvent{
			RunID:  runID,
			Total:  run.TotalTasks,
			Done:   int(doneCount.Load()),
			Failed: int(failedCount.Load()),
			Status: store.RunCompleted,
		})
		return nil
	}

	q := s.queue
	if q == nil {
		q = NewMemoryQueue(len(tasks))
	}
	if err := q.Push(ctx, tasks); err != nil {
		_ = q.Close()
		return err
	}
	_ = q.Close()

	workers := s.cfg.Concurrency
	if workers > len(tasks) {
		workers = len(tasks)
	}

	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			s.workerLoop(ctx, q, runID, run.TotalTasks, &doneCount, &failedCount, exec)
		}()
	}
	wg.Wait()

	dbCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if ctx.Err() != nil {
		_ = s.store.UpdateRunStatus(dbCtx, runID, store.RunCanceled)
		s.publishProgress(ProgressEvent{
			RunID:  runID,
			Total:  run.TotalTasks,
			Done:   int(doneCount.Load()),
			Failed: int(failedCount.Load()),
			Status: store.RunCanceled,
		})
		return ctx.Err()
	}

	if err := s.store.UpdateRunStatus(dbCtx, runID, store.RunCompleted); err != nil {
		return err
	}
	s.publishProgress(ProgressEvent{
		RunID:  runID,
		Total:  run.TotalTasks,
		Done:   int(doneCount.Load()),
		Failed: int(failedCount.Load()),
		Status: store.RunCompleted,
	})
	return nil
}

func (s *Scheduler) workerLoop(
	ctx context.Context,
	q Queue,
	runID string,
	totalTasks int,
	doneCount *atomic.Int64,
	failedCount *atomic.Int64,
	exec TaskExecutor,
) {
	for {
		if ctx.Err() != nil {
			return
		}

		task, ok, err := q.Pop(ctx)
		if err != nil || !ok {
			return
		}

		update, scores, execErr := s.executeWithRetry(ctx, task, exec)
		// 若是因为总任务被外界取消导致的退出，不写为 failed，保留状态供下次断点续跑。
		if execErr != nil && ctx.Err() != nil {
			return
		}

		dbCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if execErr != nil {
			update.Status = store.TaskFailed
			if update.ErrorMsg == "" {
				update.ErrorMsg = execErr.Error()
			}
		} else if update.Status == "" {
			update.Status = store.TaskSuccess
		}

		_ = s.store.UpdateTaskResult(dbCtx, task.ID, update)
		if len(scores) > 0 {
			_ = s.store.SaveScores(dbCtx, scores)
		}
		cancel()

		d := int(doneCount.Add(1))
		f := int(failedCount.Load())
		if update.Status == store.TaskFailed {
			f = int(failedCount.Add(1))
		}

		s.publishProgress(ProgressEvent{
			RunID:      runID,
			Total:      totalTasks,
			Done:       d,
			Failed:     f,
			Status:     store.RunRunning,
			LastTaskID: task.ID,
		})
	}
}

func (s *Scheduler) executeWithRetry(
	ctx context.Context,
	task store.EvalTask,
	exec TaskExecutor,
) (store.TaskResultUpdate, []store.EvalScore, error) {
	var lastUpdate store.TaskResultUpdate
	var lastErr error

	for attempt := 0; attempt <= s.cfg.MaxRetries; attempt++ {
		if ctx.Err() != nil {
			return lastUpdate, nil, ctx.Err()
		}

		if s.limiter != nil {
			if err := s.limiter.Wait(ctx, task.TargetName); err != nil {
				return lastUpdate, nil, err
			}
		}

		dbCtx, cancelDB := context.WithTimeout(context.Background(), 5*time.Second)
		_ = s.store.MarkTaskRunning(dbCtx, task.ID, attempt)
		cancelDB()

		taskCtx, cancelTask := context.WithTimeout(ctx, s.cfg.TaskTimeout)
		update, scores, err := exec(taskCtx, task)
		cancelTask()

		update.RetryCount = attempt
		if err == nil {
			return update, scores, nil
		}

		lastUpdate = update
		lastErr = err
		if ctx.Err() != nil {
			return lastUpdate, nil, ctx.Err()
		}

		retryAfter, retryable := IsRetryable(err)
		if !retryable || attempt == s.cfg.MaxRetries {
			return lastUpdate, scores, err
		}

		waitDur := s.cfg.BaseBackoff << attempt
		if retryAfter > 0 {
			waitDur = retryAfter
		}
		if !sleepWithContext(ctx, waitDur) {
			return lastUpdate, nil, ctx.Err()
		}
	}

	return lastUpdate, nil, lastErr
}

func (s *Scheduler) publishProgress(ev ProgressEvent) {
	if s.broker != nil {
		s.broker.Publish(ev)
	}
}

func sleepWithContext(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}
