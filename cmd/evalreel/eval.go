package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"evalreel/internal/api"
	"evalreel/internal/config"
	"evalreel/internal/report"
	"evalreel/internal/runner"
	"evalreel/internal/scheduler"
	"evalreel/internal/scorer"
	"evalreel/internal/store"
)

func runEval(args []string) {
	fs := flag.NewFlagSet("eval", flag.ExitOnError)
	cfgPath := fs.String("config", "./configs/eval.example.json", "JSON 评测任务配置文件路径")
	dbPath := fs.String("db", "./evalreel.db", "SQLite 数据库文件路径")
	resumeID := fs.String("resume", "", "需要断点续跑的历史 run_id")
	trialsOverride := fs.Int("trials", 0, "覆盖配置文件里的每题重复次数（>0 时生效）")
	_ = fs.Parse(args)

	loadDotEnv(".env")
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	st, err := store.NewSQLiteStore(*dbPath)
	if err != nil {
		logger.Error("open sqlite failed", "err", err)
		os.Exit(1)
	}
	defer st.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var runID string
	var cfg *config.EvalConfig

	if *resumeID != "" {
		run, _, _, err := st.GetRun(ctx, *resumeID)
		if err != nil {
			logger.Error("load run for resume failed", "run_id", *resumeID, "err", err)
			os.Exit(1)
		}
		var parsed config.EvalConfig
		if err := json.Unmarshal([]byte(run.ConfigJSON), &parsed); err != nil {
			logger.Error("parse stored run config failed", "err", err)
			os.Exit(1)
		}
		parsed.ApplyDefaults()
		cfg = &parsed
		runID = run.ID
		logger.Info("resuming eval run", "run_id", runID, "done", run.DoneTasks, "total", run.TotalTasks)
	} else {
		cfg, err = config.LoadEvalConfig(*cfgPath)
		if err != nil {
			logger.Error("load eval config failed", "err", err)
			os.Exit(1)
		}
		if *trialsOverride > 0 {
			cfg.Trials = *trialsOverride
		}
		runID = fmt.Sprintf("run_%s", time.Now().Format("20060102_150405"))
		tasks, err := buildEvalTasks(runID, cfg)
		if err != nil {
			logger.Error("build tasks failed", "err", err)
			os.Exit(1)
		}
		cfgBytes, _ := json.Marshal(cfg)
		run := &store.EvalRun{
			ID:         runID,
			Name:       cfg.Name,
			Mode:       cfg.Mode,
			Status:     store.RunRunning,
			ConfigJSON: string(cfgBytes),
			TotalTasks: len(tasks),
		}
		if err := st.CreateRun(ctx, run, tasks); err != nil {
			logger.Error("save run failed", "err", err)
			os.Exit(1)
		}
		logger.Info("eval run created", "run_id", runID, "mode", cfg.Mode, "total_tasks", len(tasks))
	}

	broker := scheduler.NewProgressBroker()
	evCh, unsub := broker.Subscribe(runID)
	defer unsub()

	donePrint := make(chan struct{})
	go func() {
		defer close(donePrint)
		for ev := range evCh {
			logger.Info("eval progress", "done", ev.Done, "total", ev.Total, "failed", ev.Failed, "status", ev.Status)
		}
	}()

	err = executeEvalRun(ctx, st, broker, runID, cfg, logger)
	unsub()
	<-donePrint

	if err != nil {
		if errors.Is(err, context.Canceled) {
			logger.Warn("eval canceled cleanly; resume later with -resume", "run_id", runID)
			return
		}
		logger.Error("eval failed", "err", err)
		os.Exit(1)
	}
}

func runServe(args []string) {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	listen := fs.String("listen", "127.0.0.1:8081", "Gin 控制面接口监听地址")
	dbPath := fs.String("db", "./evalreel.db", "SQLite 数据库文件路径")
	_ = fs.Parse(args)

	loadDotEnv(".env")
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	st, err := store.NewSQLiteStore(*dbPath)
	if err != nil {
		logger.Error("open sqlite failed", "err", err)
		os.Exit(1)
	}
	defer st.Close()

	broker := scheduler.NewProgressBroker()
	apiSrv := api.NewServer(
		st,
		broker,
		func(_ context.Context, runID string, cfg *config.EvalConfig) ([]store.EvalTask, error) {
			return buildEvalTasks(runID, cfg)
		},
		func(ctx context.Context, runID string, cfg *config.EvalConfig) {
			if err := executeEvalRun(ctx, st, broker, runID, cfg, logger); err != nil {
				logger.Warn("background eval run stopped", "run_id", runID, "err", err)
			}
		},
	)

	srv := &http.Server{
		Addr:              *listen,
		Handler:           apiSrv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Info("evalreel api server started", "addr", *listen, "db", *dbPath)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("api server failed", "err", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down api server")
	sdCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(sdCtx)
	apiSrv.Shutdown()
}

func buildEvalTasks(runID string, cfg *config.EvalConfig) ([]store.EvalTask, error) {
	var items []runner.DatasetItem
	var err error

	if cfg.Mode == "fork" {
		item, fErr := runner.LoadForkItem(cfg.ForkSource.TraceFile, cfg.ForkSource.TraceID, cfg.ForkSource.StepIndex)
		if fErr != nil {
			return nil, fErr
		}
		items = []runner.DatasetItem{item}
	} else {
		if cfg.DatasetPath == "" {
			return nil, errors.New("dataset_path is required")
		}
		items, err = runner.LoadDataset(cfg.DatasetPath)
		if err != nil {
			return nil, err
		}
	}

	tasks := make([]store.EvalTask, 0, len(items)*len(cfg.Targets)*cfg.Trials)
	for _, item := range items {
		snapBytes, _ := json.Marshal(item)
		for _, tgt := range cfg.Targets {
			for k := 0; k < cfg.Trials; k++ {
				tasks = append(tasks, store.EvalTask{
					ID:              fmt.Sprintf("%s_%s_%s_k%d", runID, item.ID, tgt.Name, k),
					RunID:           runID,
					ItemID:          item.ID,
					TargetName:      tgt.Name,
					TrialIndex:      k,
					Status:          store.TaskPending,
					RequestSnapshot: string(snapBytes),
				})
			}
		}
	}
	return tasks, nil
}

func executeEvalRun(
	ctx context.Context,
	st store.Store,
	broker *scheduler.ProgressBroker,
	runID string,
	cfg *config.EvalConfig,
	logger *slog.Logger,
) error {
	targetMap := make(map[string]config.TargetConfig, len(cfg.Targets))
	rpmMap := make(map[string]int, len(cfg.Targets))
	for _, tgt := range cfg.Targets {
		targetMap[tgt.Name] = tgt
		rpmMap[tgt.Name] = tgt.RPM
	}

	scorers, err := scorer.BuildScorers(cfg.Scorers)
	if err != nil {
		return err
	}
	defer scorer.CloseScorers(scorers)

	llmRunner := runner.NewLLMRunner()
	defer llmRunner.CloseIdleConnections()

	sched := scheduler.New(scheduler.Config{
		Concurrency: cfg.Concurrency,
		MaxRetries:  cfg.MaxRetries,
		TaskTimeout: time.Duration(cfg.TaskTimeoutSec) * time.Second,
		BaseBackoff: 1 * time.Second,
	}, st, scheduler.NewLocalLimiter(rpmMap), broker)

	err = sched.Run(ctx, runID, func(taskCtx context.Context, tk store.EvalTask) (store.TaskResultUpdate, []store.EvalScore, error) {
		tgt := targetMap[tk.TargetName]
		var item runner.DatasetItem
		if uErr := json.Unmarshal([]byte(tk.RequestSnapshot), &item); uErr != nil {
			return store.TaskResultUpdate{}, nil, fmt.Errorf("unmarshal snapshot: %w", uErr)
		}

		var out runner.RunOutput
		var execErr error
		if cfg.Mode == "agent" {
			agentCfg := cfg.Agent
			if tgt.Agent != nil && tgt.Agent.Command != "" {
				agentCfg = *tgt.Agent
			}
			out, execErr = runner.NewAgentRunner(agentCfg).Execute(taskCtx, tgt, item)
		} else {
			out, execErr = llmRunner.Execute(taskCtx, tgt, item)
		}

		if out.WorkDir != "" {
			defer os.RemoveAll(out.WorkDir)
		}
		if execErr != nil {
			return store.TaskResultUpdate{
				TotalMs:  out.TotalMs,
				ErrorMsg: execErr.Error(),
			}, nil, execErr
		}

		var scores []store.EvalScore
		for _, sc := range scorers {
			sv, sErr := sc.Score(taskCtx, item, out)
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
		return store.TaskResultUpdate{
			Status:        store.TaskSuccess,
			TraceID:       out.TraceID,
			ReasoningText: out.ReasoningText,
			ResponseText:  out.ResponseText,
			ToolCallsJSON: string(toolsBytes),
			TTFTMs:        out.TTFTMs,
			TotalMs:       out.TotalMs,
			InputTokens:   out.InputTokens,
			OutputTokens:  out.OutputTokens,
			CostUSD:       out.CostUSD,
		}, scores, nil
	})
	if err != nil {
		return err
	}

	run, tasks, scores, err := st.GetRun(ctx, runID)
	if err != nil {
		return err
	}
	summary := report.Aggregate(run, tasks, scores)
	htmlStr, err := report.RenderHTML(summary)
	if err != nil {
		return err
	}
	summaryBytes, _ := json.Marshal(summary)
	if err := st.SaveReport(ctx, &store.EvalReport{
		RunID:       runID,
		SummaryJSON: string(summaryBytes),
		HTMLContent: htmlStr,
	}); err != nil {
		return err
	}

	outFile, err := report.WriteHTMLFile(cfg.OutputDir, runID, htmlStr)
	if err != nil {
		return err
	}
	absPath, _ := filepath.Abs(outFile)

	fmt.Printf("\n=== EvalReel 评测完成 (%s) ===\n", runID)
	for _, ts := range summary.Targets {
		fmt.Printf("- %-26s | 准确率: %5.1f%% | pass@%d: %5.1f%% | pass^%d: %5.1f%% | P50/P95: %d/%dms | 词元: %d/%d | 花费: %.4f\n",
			ts.Name, ts.Accuracy, summary.Trials, ts.PassAtK, summary.Trials, ts.PassPowerK,
			ts.P50Ms, ts.P95Ms, ts.TotalInputTokens, ts.TotalOutputTokens, ts.TotalCostUSD)
	}
	logger.Info("report generated", "path", absPath)
	return nil
}

func loadDotEnv(path string) {
	data, err := os.ReadFile(path)
	if err != nil {
		return
	}
	for _, raw := range strings.Split(string(data), "\n") {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") || !strings.Contains(line, "=") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		k := strings.TrimSpace(parts[0])
		v := strings.Trim(strings.TrimSpace(parts[1]), `"'`)
		if os.Getenv(k) == "" {
			_ = os.Setenv(k, v)
		}
	}
}
