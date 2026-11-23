package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// SQLiteStore 是 Store 接口的本地零配置实现，底层使用纯 Go SQLite 驱动。
type SQLiteStore struct {
	db *gorm.DB
}

// NewSQLiteStore 打开或创建 SQLite 数据库文件，开启 WAL 模式并自动建表。
func NewSQLiteStore(dbPath string) (*SQLiteStore, error) {
	if dbPath != ":memory:" {
		dir := filepath.Dir(dbPath)
		if dir != "" && dir != "." {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return nil, fmt.Errorf("create db dir %s: %w", dir, err)
			}
		}
	}

	dsn := dbPath
	if dbPath != ":memory:" {
		dsn = dbPath + "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)"
	}

	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		return nil, fmt.Errorf("open sqlite %s: %w", dbPath, err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("get underlying sql.DB: %w", err)
	}
	// 限制本地单文件数据库只保留 1 个连接排队写入，彻底避免多协程并发写库时出现 SQLITE_BUSY 报错。
	sqlDB.SetMaxOpenConns(1)

	if err := db.AutoMigrate(&EvalRun{}, &EvalTask{}, &EvalScore{}, &EvalReport{}); err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("auto migrate tables: %w", err)
	}

	return &SQLiteStore{db: db}, nil
}

// CreateRun 在单个事务内同时写入 1 条总任务记录和拆分后的全部子任务记录。
func (s *SQLiteStore) CreateRun(ctx context.Context, run *EvalRun, tasks []EvalTask) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(run).Error; err != nil {
			return fmt.Errorf("create eval_run: %w", err)
		}
		if len(tasks) > 0 {
			if err := tx.CreateInBatches(&tasks, 100).Error; err != nil {
				return fmt.Errorf("create eval_tasks: %w", err)
			}
		}
		return nil
	})
}

// ClaimPendingTasks 查出上次未完成或因崩溃停在 running 状态的子任务，用于断点续跑。
func (s *SQLiteStore) ClaimPendingTasks(ctx context.Context, runID string) ([]EvalTask, error) {
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

// MarkTaskRunning 将指定子任务状态流转为 running，并记录当前重试次数。
func (s *SQLiteStore) MarkTaskRunning(ctx context.Context, taskID string, retryCount int) error {
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
		return ErrNotFound
	}
	return nil
}

// UpdateTaskResult 在事务中更新子任务执行结果，并幂等地累加总任务完成/失败计数。
func (s *SQLiteStore) UpdateTaskResult(ctx context.Context, taskID string, update TaskResultUpdate) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var existing EvalTask
		if err := tx.Select("id", "run_id", "status").Where("id = ?", taskID).First(&existing).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrNotFound
			}
			return fmt.Errorf("query task %s: %w", taskID, err)
		}

		err := tx.Model(&EvalTask{}).Where("id = ?", taskID).Updates(map[string]any{
			"status":         update.Status,
			"retry_count":    update.RetryCount,
			"trace_id":       update.TraceID,
			"reasoning_text": update.ReasoningText,
			"response_text":  update.ResponseText,
			"tool_calls_json": update.ToolCallsJSON,
			"ttft_ms":        update.TTFTMs,
			"total_ms":       update.TotalMs,
			"input_tokens":   update.InputTokens,
			"output_tokens":  update.OutputTokens,
			"cost_usd":       update.CostUSD,
			"error_msg":      update.ErrorMsg,
		}).Error
		if err != nil {
			return fmt.Errorf("update task %s: %w", taskID, err)
		}

		// 仅当任务首次从 pending/running 进入终态时才递增计数，防止重复写入导致进度超过 100%。
		wasActive := existing.Status == TaskPending || existing.Status == TaskRunning
		nowFinal := update.Status == TaskSuccess || update.Status == TaskFailed
		if wasActive && nowFinal {
			updates := map[string]any{
				"done_tasks": gorm.Expr("done_tasks + 1"),
			}
			if update.Status == TaskFailed {
				updates["failed_tasks"] = gorm.Expr("failed_tasks + 1")
			}
			if err := tx.Model(&EvalRun{}).Where("id = ?", existing.RunID).Updates(updates).Error; err != nil {
				return fmt.Errorf("increment run counters: %w", err)
			}
		}
		return nil
	})
}

// UpdateRunStatus 更新总任务的运行状态，并在到达终态时写入完成时间。
func (s *SQLiteStore) UpdateRunStatus(ctx context.Context, runID string, status RunStatus) error {
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
		return ErrNotFound
	}
	return nil
}

// SaveScores 批量保存子任务的批卷结果。
func (s *SQLiteStore) SaveScores(ctx context.Context, scores []EvalScore) error {
	if len(scores) == 0 {
		return nil
	}
	if err := s.db.WithContext(ctx).Create(&scores).Error; err != nil {
		return fmt.Errorf("save scores: %w", err)
	}
	return nil
}

// GetRun 读取指定总任务及关联的全部子任务与评分记录。
func (s *SQLiteStore) GetRun(ctx context.Context, runID string) (*EvalRun, []EvalTask, []EvalScore, error) {
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

// ListRuns 按创建时间倒序列出历史评测总任务。
func (s *SQLiteStore) ListRuns(ctx context.Context, limit int) ([]EvalRun, error) {
	if limit <= 0 {
		limit = 20
	}
	var runs []EvalRun
	if err := s.db.WithContext(ctx).Order("created_at DESC").Limit(limit).Find(&runs).Error; err != nil {
		return nil, fmt.Errorf("list runs: %w", err)
	}
	return runs, nil
}

// SaveReport 保存或更新指定任务的汇总报告与 HTML 内容。
func (s *SQLiteStore) SaveReport(ctx context.Context, report *EvalReport) error {
	if err := s.db.WithContext(ctx).Save(report).Error; err != nil {
		return fmt.Errorf("save report: %w", err)
	}
	return nil
}

// GetReport 读取缓存的评测报告。
func (s *SQLiteStore) GetReport(ctx context.Context, runID string) (*EvalReport, error) {
	var report EvalReport
	if err := s.db.WithContext(ctx).Where("run_id = ?", runID).First(&report).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("get report: %w", err)
	}
	return &report, nil
}

// Close 关闭底层数据库连接。
func (s *SQLiteStore) Close() error {
	sqlDB, err := s.db.DB()
	if err != nil {
		return err
	}
	return sqlDB.Close()
}
