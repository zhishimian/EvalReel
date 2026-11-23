package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"time"

	"evalreel/internal/config"
	"evalreel/internal/report"
	"evalreel/internal/runner"
	"evalreel/internal/scheduler"
	"evalreel/internal/scorer"
	"evalreel/internal/store"

	"github.com/hibiken/asynq"
)

// Config 配置分布式 Worker 节点连接参数、并发度与报告输出目录。
type Config struct {
	RedisAddr     string
	RedisPassword string
	RedisDB       int
	Concurrency   int
	OutputDir     string
	Logger        *slog.Logger
}

// Processor 负责从 Asynq 队列消费单条子任务，完成限流、执行、批卷、回写数据库以及唯一收口报告生成。
type Processor struct {
	store       store.Store
	limiter     *scheduler.RedisLuaLimiter
	bus         scheduler.ProgressBus
	llmRunner   *runner.LLMRunner
	concurrency int
	outputDir   string
	logger      *slog.Logger

	cfgMu       sync.RWMutex
	runCfgCache map[string]*config.EvalConfig
}

// NewProcessor 创建一个可用于独立 Worker 进程或单元测试的任务处理器。
func NewProcessor(
	st store.Store,
	lim *scheduler.RedisLuaLimiter,
	bus scheduler.ProgressBus,
	concurrency int,
	outputDir string,
	logger *slog.Logger,
) *Processor {
	if concurrency <= 0 {
		concurrency = 10
	}
	if outputDir == "" {
		outputDir = "./reports"
	}
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Processor{
		store:       st,
		limiter:     lim,
		bus:         bus,
		llmRunner:   runner.NewLLMRunner(),
		concurrency: concurrency,
		outputDir:   outputDir,
		logger:      logger,
		runCfgCache: make(map[string]*config.EvalConfig),
	}
}

// Close 释放处理器内部维护的 HTTP 长连接池。
func (p *Processor) Close() {
	if p.llmRunner != nil {
		p.llmRunner.CloseIdleConnections()
	}
}

// RunServer 启动 Asynq 分布式消费节点，阻塞运行直到上下文被取消后执行优雅停机。
func (p *Processor) RunServer(ctx context.Context, redisAddr, redisPassword string, redisDB int) error {
	srv := asynq.NewServer(
		asynq.RedisClientOpt{
			Addr:     redisAddr,
			Password: redisPassword,
			DB:       redisDB,
		},
		asynq.Config{
			Concurrency: p.concurrency,
			RetryDelayFunc: func(n int, err error, t *asynq.Task) time.Duration {
				if retryAfter, ok := scheduler.IsRetryable(err); ok && retryAfter > 0 {
					return retryAfter
				}
				return asynq.DefaultRetryDelayFunc(n, err, t)
			},
			ErrorHandler: asynq.ErrorHandlerFunc(func(errCtx context.Context, task *asynq.Task, err error) {
				p.handleFinalAsynqError(errCtx, task, err)
			}),
		},
	)

	mux := asynq.NewServeMux()
	mux.HandleFunc(scheduler.TaskTypeEval, p.ProcessTask)

	if err := srv.Start(mux); err != nil {
		return fmt.Errorf("start asynq server: %w", err)
	}
	p.logger.Info("evalreel worker started", "redis", redisAddr, "concurrency", p.concurrency)

	<-ctx.Done()
	p.logger.Info("shutting down worker gracefully")
	srv.Shutdown()
	p.Close()
	return nil
}

// ProcessTask 执行单条子评测任务：限流排队 -> 模型或 Agent 调用 -> 批卷 -> 回写状态与广播进度。
func (p *Processor) ProcessTask(ctx context.Context, t *asynq.Task) error {
	var payload scheduler.EvalTaskPayload
	if err := json.Unmarshal(t.Payload(), &payload); err != nil {
		return fmt.Errorf("%w: unmarshal payload: %v", asynq.SkipRetry, err)
	}

	tk, err := p.store.GetTask(ctx, payload.TaskID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return fmt.Errorf("%w: task %s not found", asynq.SkipRetry, payload.TaskID)
		}
		return err
	}
	if tk.Status == store.TaskSuccess || tk.Status == store.TaskFailed {
		return nil
	}

	cfg, err := p.getRunConfig(ctx, payload.RunID)
	if err != nil {
		return err
	}
	tgt, ok := findTarget(cfg.Targets, tk.TargetName)
	if !ok {
		failErr := fmt.Errorf("target %q not found in run config", tk.TargetName)
		p.recordFailure(payload.RunID, tk.ID, 0, failErr.Error(), 0)
		return fmt.Errorf("%w: %v", asynq.SkipRetry, failErr)
	}

	var item runner.DatasetItem
	if err := json.Unmarshal([]byte(tk.RequestSnapshot), &item); err != nil {
		p.recordFailure(payload.RunID, tk.ID, 0, err.Error(), 0)
		return fmt.Errorf("%w: unmarshal snapshot: %v", asynq.SkipRetry, err)
	}

	if p.limiter != nil && tgt.RPM > 0 {
		if err := p.limiter.WaitFor(ctx, tgt.Name, tgt.RPM, 0); err != nil {
			return err
		}
	}

	retryCount, _ := asynq.GetRetryCount(ctx)
	maxRetry, hasMaxRetry := asynq.GetMaxRetry(ctx)

	dbCtx, cancelMark := context.WithTimeout(context.Background(), 5*time.Second)
	_ = p.store.MarkTaskRunning(dbCtx, tk.ID, retryCount)
	cancelMark()

	taskTimeout := time.Duration(cfg.TaskTimeoutSec) * time.Second
	if taskTimeout <= 0 {
		taskTimeout = 120 * time.Second
	}
	execCtx, cancelExec := context.WithTimeout(ctx, taskTimeout)
	defer cancelExec()

	var out runner.RunOutput
	var execErr error
	if cfg.Mode == "agent" {
		agentCfg := cfg.Agent
		if tgt.Agent != nil && tgt.Agent.Command != "" {
			agentCfg = *tgt.Agent
		}
		out, execErr = runner.NewAgentRunner(agentCfg).Execute(execCtx, tgt, item)
	} else {
		out, execErr = p.llmRunner.Execute(execCtx, tgt, item)
	}
	if out.WorkDir != "" {
		defer os.RemoveAll(out.WorkDir)
	}

	if execErr != nil {
		if ctx.Err() != nil {
			return execErr
		}
		_, retryable := scheduler.IsRetryable(execErr)
		if retryable && (!hasMaxRetry || retryCount < maxRetry) {
			return execErr
		}
		p.recordFailure(payload.RunID, tk.ID, retryCount, execErr.Error(), out.TotalMs)
		if !retryable {
			return fmt.Errorf("%w: %v", asynq.SkipRetry, execErr)
		}
		return execErr
	}

	scorers, err := scorer.BuildScorers(cfg.Scorers)
	if err != nil {
		p.recordFailure(payload.RunID, tk.ID, retryCount, err.Error(), out.TotalMs)
		return fmt.Errorf("%w: build scorers: %v", asynq.SkipRetry, err)
	}
	defer scorer.CloseScorers(scorers)

	var scores []store.EvalScore
	for _, sc := range scorers {
		sv, sErr := sc.Score(execCtx, item, out)
		if sErr != nil {
			sv = store.EvalScore{
				ScorerName: sc.Name(),
				Passed:     false,
				Reason:     "scorer error: " + sErr.Error(),
			}
		}
		sv.TaskID = tk.ID
		sv.RunID = tk.RunID
		scores = append(scores, sv)
	}

	toolsBytes, _ := json.Marshal(out.ToolCalls)
	saveCtx, cancelSave := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelSave()

	if err := p.store.UpdateTaskResult(saveCtx, tk.ID, store.TaskResultUpdate{
		Status:        store.TaskSuccess,
		RetryCount:    retryCount,
		TraceID:       out.TraceID,
		ReasoningText: out.ReasoningText,
		ResponseText:  out.ResponseText,
		ToolCallsJSON: string(toolsBytes),
		TTFTMs:        out.TTFTMs,
		TotalMs:       out.TotalMs,
		InputTokens:   out.InputTokens,
		OutputTokens:  out.OutputTokens,
		CostUSD:       out.CostUSD,
	}); err != nil {
		return err
	}
	if len(scores) > 0 {
		if err := p.store.SaveScores(saveCtx, scores); err != nil {
			p.logger.Warn("save scores failed", "task_id", tk.ID, "err", err)
		}
	}

	p.publishProgressAndTryFinalize(saveCtx, payload.RunID, tk.ID)
	return nil
}

func (p *Processor) handleFinalAsynqError(_ context.Context, task *asynq.Task, err error) {
	if task == nil || errors.Is(err, asynq.SkipRetry) {
		return
	}
	var payload scheduler.EvalTaskPayload
	if uErr := json.Unmarshal(task.Payload(), &payload); uErr != nil {
		return
	}
	tk, gErr := p.store.GetTask(context.Background(), payload.TaskID)
	if gErr != nil || tk.Status == store.TaskSuccess || tk.Status == store.TaskFailed {
		return
	}
	run, rErr := p.store.GetRunMeta(context.Background(), payload.RunID)
	if rErr != nil {
		return
	}
	var cfg config.EvalConfig
	_ = json.Unmarshal([]byte(run.ConfigJSON), &cfg)
	if tk.RetryCount >= cfg.MaxRetries {
		p.recordFailure(payload.RunID, payload.TaskID, tk.RetryCount, err.Error(), 0)
	}
}

func (p *Processor) recordFailure(runID, taskID string, retryCount int, errMsg string, totalMs int64) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_ = p.store.UpdateTaskResult(ctx, taskID, store.TaskResultUpdate{
		Status:     store.TaskFailed,
		RetryCount: retryCount,
		TotalMs:    totalMs,
		ErrorMsg:   errMsg,
	})
	p.publishProgressAndTryFinalize(ctx, runID, taskID)
}

func (p *Processor) publishProgressAndTryFinalize(ctx context.Context, runID, lastTaskID string) {
	meta, err := p.store.GetRunMeta(ctx, runID)
	if err != nil {
		return
	}
	if p.bus != nil {
		_ = p.bus.PublishEvent(ctx, scheduler.ProgressEvent{
			RunID:      runID,
			Total:      meta.TotalTasks,
			Done:       meta.DoneTasks,
			Failed:     meta.FailedTasks,
			Status:     meta.Status,
			LastTaskID: lastTaskID,
			Timestamp:  time.Now().UTC(),
		})
	}
	if meta.DoneTasks < meta.TotalTasks {
		return
	}

	won, err := p.store.TryFinalizeRun(ctx, runID, time.Now().UTC())
	if err != nil || !won {
		return
	}

	run, tasks, scores, err := p.store.GetRun(ctx, runID)
	if err != nil {
		p.logger.Warn("load run for report failed", "run_id", runID, "err", err)
		return
	}
	summary := report.Aggregate(run, tasks, scores)
	htmlStr, err := report.RenderHTML(summary)
	if err != nil {
		p.logger.Warn("render report failed", "run_id", runID, "err", err)
		return
	}
	summaryBytes, _ := json.Marshal(summary)
	if err := p.store.SaveReport(ctx, &store.EvalReport{
		RunID:       runID,
		SummaryJSON: string(summaryBytes),
		HTMLContent: htmlStr,
	}); err != nil {
		p.logger.Warn("save report to store failed", "run_id", runID, "err", err)
	}

	outDir := p.outputDir
	if cfg, cErr := p.getRunConfig(ctx, runID); cErr == nil && cfg.OutputDir != "" {
		outDir = cfg.OutputDir
	}
	if _, wErr := report.WriteHTMLFile(outDir, runID, htmlStr); wErr != nil {
		p.logger.Warn("write report file failed", "run_id", runID, "err", wErr)
	}

	if p.bus != nil {
		_ = p.bus.PublishEvent(ctx, scheduler.ProgressEvent{
			RunID:      runID,
			Total:      run.TotalTasks,
			Done:       run.DoneTasks,
			Failed:     run.FailedTasks,
			Status:     store.RunCompleted,
			LastTaskID: lastTaskID,
			Timestamp:  time.Now().UTC(),
		})
	}
	p.logger.Info("eval run finalized by worker", "run_id", runID, "done", run.DoneTasks, "failed", run.FailedTasks)
}

func (p *Processor) getRunConfig(ctx context.Context, runID string) (*config.EvalConfig, error) {
	p.cfgMu.RLock()
	cached, ok := p.runCfgCache[runID]
	p.cfgMu.RUnlock()
	if ok {
		return cached, nil
	}

	meta, err := p.store.GetRunMeta(ctx, runID)
	if err != nil {
		return nil, err
	}
	var cfg config.EvalConfig
	if err := json.Unmarshal([]byte(meta.ConfigJSON), &cfg); err != nil {
		return nil, fmt.Errorf("%w: parse run config: %v", asynq.SkipRetry, err)
	}
	cfg.ApplyDefaults()

	p.cfgMu.Lock()
	p.runCfgCache[runID] = &cfg
	p.cfgMu.Unlock()
	return &cfg, nil
}

func findTarget(targets []config.TargetConfig, name string) (config.TargetConfig, bool) {
	for _, tgt := range targets {
		if tgt.Name == name {
			return tgt, true
		}
	}
	return config.TargetConfig{}, false
}
