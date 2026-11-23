package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// gormStore 封装基于 Gorm 的通用存储逻辑，供 SQLiteStore 与 MySQLStore 共同复用。
type gormStore struct {
	db *gorm.DB
}

func (s *gormStore) autoMigrate() error {
	if err := s.db.AutoMigrate(&EvalRun{}, &EvalTask{}, &EvalScore{}, &EvalReport{}, &TraceSpan{}); err != nil {
		return fmt.Errorf("auto migrate tables: %w", err)
	}
	return nil
}

func isDuplicateKeyError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, gorm.ErrDuplicatedKey) {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "Error 1062") ||
		strings.Contains(msg, "Duplicate entry") ||
		strings.Contains(msg, "UNIQUE constraint failed")
}

// CreateRun 在单个事务内同时写入 1 条总任务记录和拆分后的全部子任务记录。
func (s *gormStore) CreateRun(ctx context.Context, run *EvalRun, tasks []EvalTask) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(run).Error; err != nil {
			if isDuplicateKeyError(err) {
				return fmt.Errorf("%w: %v", ErrDuplicateKey, err)
			}
			return fmt.Errorf("create eval_run: %w", err)
		}
		if len(tasks) > 0 {
			if err := tx.CreateInBatches(&tasks, 100).Error; err != nil {
				if isDuplicateKeyError(err) {
					return fmt.Errorf("%w: %v", ErrDuplicateKey, err)
				}
				return fmt.Errorf("create eval_tasks: %w", err)
			}
		}
		return nil
	})
}

// FindRunByIdempotencyKey 按请求幂等键查找已创建的总任务记录，未命中返回 ErrNotFound。
func (s *gormStore) FindRunByIdempotencyKey(ctx context.Context, key string) (*EvalRun, error) {
	if key == "" {
		return nil, ErrNotFound
	}
	var run EvalRun
	if err := s.db.WithContext(ctx).Where("idempotency_key = ?", key).First(&run).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("find run by idempotency key: %w", err)
	}
	return &run, nil
}

// ClaimPendingTasks 查出上次未完成或因崩溃停在 running 状态的子任务，用于断点续跑。
func (s *gormStore) ClaimPendingTasks(ctx context.Context, runID string) ([]EvalTask, error) {
	var tasks []EvalTask
	err := s.db.WithContext(ctx).
		Where("run_id = ? AND status IN ?", runID, []TaskStatus{TaskPending, TaskRunning}).
		Order("trial_index ASC, id ASC").
		Find(&tasks).Error
	if err != nil {
		return nil, fmt.Errorf("claim pending tasks: %w", err)
	}
	return tasks, nil
}

// GetTask 按主键查询单条子任务详情，供分布式 Worker 节点拉取执行。
func (s *gormStore) GetTask(ctx context.Context, taskID string) (*EvalTask, error) {
	var tk EvalTask
	if err := s.db.WithContext(ctx).Where("id = ?", taskID).First(&tk).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("get task %s: %w", taskID, err)
	}
	return &tk, nil
}

// MarkTaskRunning 将指定子任务状态流转为 running，并记录当前重试次数。
func (s *gormStore) MarkTaskRunning(ctx context.Context, taskID string, retryCount int) error {
	res := s.db.WithContext(ctx).Model(&EvalTask{}).
		Where("id = ?", taskID).
		Updates(map[string]any{
			"status":      TaskRunning,
			"retry_count": retryCount,
		})
	if res.Error != nil {
		return fmt.Errorf("mark task running: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		var count int64
		if err := s.db.WithContext(ctx).Model(&EvalTask{}).Where("id = ?", taskID).Count(&count).Error; err != nil {
			return fmt.Errorf("check task exists %s: %w", taskID, err)
		}
		if count == 0 {
			return ErrNotFound
		}
	}
	return nil
}

// UpdateTaskResult 通过条件更新保证状态机并发安全，并幂等地递增总任务计数。
func (s *gormStore) UpdateTaskResult(ctx context.Context, taskID string, update TaskResultUpdate) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		res := tx.Model(&EvalTask{}).
			Where("id = ? AND status IN ?", taskID, []TaskStatus{TaskPending, TaskRunning}).
			Updates(map[string]any{
				"status":          update.Status,
				"retry_count":     update.RetryCount,
				"trace_id":        update.TraceID,
				"reasoning_text":  update.ReasoningText,
				"response_text":   update.ResponseText,
				"tool_calls_json": update.ToolCallsJSON,
				"ttft_ms":         update.TTFTMs,
				"total_ms":        update.TotalMs,
				"input_tokens":    update.InputTokens,
				"output_tokens":   update.OutputTokens,
				"cost_usd":        update.CostUSD,
				"error_msg":       update.ErrorMsg,
			})
		if res.Error != nil {
			return fmt.Errorf("update task %s: %w", taskID, res.Error)
		}

		// 若影响行数为 0，需要区分“任务不存在”与“已经被其他 Worker 抢先写为终态”。
		if res.RowsAffected == 0 {
			var count int64
			if err := tx.Model(&EvalTask{}).Where("id = ?", taskID).Count(&count).Error; err != nil {
				return fmt.Errorf("query task %s: %w", taskID, err)
			}
			if count == 0 {
				return ErrNotFound
			}
			return nil
		}

		if update.Status == TaskSuccess || update.Status == TaskFailed {
			var runID string
			if err := tx.Model(&EvalTask{}).Select("run_id").Where("id = ?", taskID).Scan(&runID).Error; err != nil {
				return fmt.Errorf("query task run_id %s: %w", taskID, err)
			}
			counters := map[string]any{
				"done_tasks": gorm.Expr("done_tasks + 1"),
			}
			if update.Status == TaskFailed {
				counters["failed_tasks"] = gorm.Expr("failed_tasks + 1")
			}
			if err := tx.Model(&EvalRun{}).Where("id = ?", runID).Updates(counters).Error; err != nil {
				return fmt.Errorf("increment run counters: %w", err)
			}
		}
		return nil
	})
}

// UpdateRunStatus 更新总任务的运行状态，并在到达终态时写入完成时间。
func (s *gormStore) UpdateRunStatus(ctx context.Context, runID string, status RunStatus) error {
	updates := map[string]any{
		"status": status,
	}
	if status == RunCompleted || status == RunFailed || status == RunCanceled {
		now := time.Now().UTC()
		updates["finished_at"] = &now
	}
	res := s.db.WithContext(ctx).Model(&EvalRun{}).Where("id = ?", runID).Updates(updates)
	if res.Error != nil {
		return fmt.Errorf("update run status: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		var count int64
		if err := s.db.WithContext(ctx).Model(&EvalRun{}).Where("id = ?", runID).Count(&count).Error; err != nil {
			return fmt.Errorf("check run exists %s: %w", runID, err)
		}
		if count == 0 {
			return ErrNotFound
		}
	}
	return nil
}

// TryFinalizeRun 在多 Worker 场景下以条件更新竞争整场评测的唯一收口权，仅一个调用者会返回 true。
func (s *gormStore) TryFinalizeRun(ctx context.Context, runID string, finishedAt time.Time) (bool, error) {
	if finishedAt.IsZero() {
		finishedAt = time.Now().UTC()
	} else {
		finishedAt = finishedAt.UTC()
	}
	res := s.db.WithContext(ctx).Model(&EvalRun{}).
		Where("id = ? AND status = ? AND done_tasks >= total_tasks", runID, RunRunning).
		Updates(map[string]any{
			"status":      RunCompleted,
			"finished_at": &finishedAt,
		})
	if res.Error != nil {
		return false, fmt.Errorf("try finalize run %s: %w", runID, res.Error)
	}
	return res.RowsAffected == 1, nil
}

// SaveScores 批量保存子任务的批卷结果。
func (s *gormStore) SaveScores(ctx context.Context, scores []EvalScore) error {
	if len(scores) == 0 {
		return nil
	}
	if err := s.db.WithContext(ctx).Create(&scores).Error; err != nil {
		return fmt.Errorf("save scores: %w", err)
	}
	return nil
}

// GetRun 读取指定总任务及关联的全部子任务与评分记录。
func (s *gormStore) GetRun(ctx context.Context, runID string) (*EvalRun, []EvalTask, []EvalScore, error) {
	var run EvalRun
	if err := s.db.WithContext(ctx).Where("id = ?", runID).First(&run).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil, nil, ErrNotFound
		}
		return nil, nil, nil, fmt.Errorf("get run: %w", err)
	}

	var tasks []EvalTask
	if err := s.db.WithContext(ctx).Where("run_id = ?", runID).Order("id ASC").Find(&tasks).Error; err != nil {
		return nil, nil, nil, fmt.Errorf("get run tasks: %w", err)
	}

	var scores []EvalScore
	if err := s.db.WithContext(ctx).Where("run_id = ?", runID).Order("id ASC").Find(&scores).Error; err != nil {
		return nil, nil, nil, fmt.Errorf("get run scores: %w", err)
	}

	return &run, tasks, scores, nil
}

// GetRunMeta 仅读取评测总任务元信息（不拉取全量子任务行），供 Worker 快速解析配置与检查收口状态。
func (s *gormStore) GetRunMeta(ctx context.Context, runID string) (*EvalRun, error) {
	var run EvalRun
	if err := s.db.WithContext(ctx).Where("id = ?", runID).First(&run).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("get run meta: %w", err)
	}
	return &run, nil
}

// ListRuns 按创建时间倒序列出历史评测总任务。
func (s *gormStore) ListRuns(ctx context.Context, limit int) ([]EvalRun, error) {
	if limit <= 0 {
		limit = 20
	}
	var runs []EvalRun
	if err := s.db.WithContext(ctx).Order("created_at DESC").Limit(limit).Find(&runs).Error; err != nil {
		return nil, fmt.Errorf("list runs: %w", err)
	}
	return runs, nil
}

// SaveReport 幂等保存或更新指定任务的汇总报告与 HTML 内容。
func (s *gormStore) SaveReport(ctx context.Context, report *EvalReport) error {
	if err := s.db.WithContext(ctx).Clauses(clause.OnConflict{UpdateAll: true}).Create(report).Error; err != nil {
		return fmt.Errorf("save report: %w", err)
	}
	return nil
}

// GetReport 读取缓存的评测报告。
func (s *gormStore) GetReport(ctx context.Context, runID string) (*EvalReport, error) {
	var report EvalReport
	if err := s.db.WithContext(ctx).Where("run_id = ?", runID).First(&report).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("get report: %w", err)
	}
	return &report, nil
}

// SaveSpans 批量保存或更新多 Agent 调用链节点记录。
func (s *gormStore) SaveSpans(ctx context.Context, spans []TraceSpan) error {
	if len(spans) == 0 {
		return nil
	}
	if err := s.db.WithContext(ctx).Clauses(clause.OnConflict{UpdateAll: true}).Create(&spans).Error; err != nil {
		return fmt.Errorf("save spans: %w", err)
	}
	return nil
}

// ListSpansByTraceID 按开始时间升序读取同一条调用链下的所有节点记录。
func (s *gormStore) ListSpansByTraceID(ctx context.Context, traceID string) ([]TraceSpan, error) {
	var spans []TraceSpan
	if err := s.db.WithContext(ctx).Where("trace_id = ?", traceID).Order("start_time_ms ASC, span_id ASC").Find(&spans).Error; err != nil {
		return nil, fmt.Errorf("list spans by trace_id: %w", err)
	}
	return spans, nil
}

// Close 关闭底层数据库连接。
func (s *gormStore) Close() error {
	sqlDB, err := s.db.DB()
	if err != nil {
		return err
	}
	return sqlDB.Close()
}
