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
	"evalreel/internal/observability"
	"evalreel/internal/report"
	"evalreel/internal/runner"
	"evalreel/internal/scheduler"
	"evalreel/internal/scorer"
	"evalreel/internal/store"

	"github.com/redis/go-redis/v9"
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
	loadDotEnv(".env")
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	mode := fs.String("mode", envOrDefault("EVALREEL_MODE", "local"), "运行模式: local（单机 SQLite）或 server（MySQL + Redis 分布式）")
	listen := fs.String("listen", envOrDefault("EVALREEL_LISTEN", "127.0.0.1:8081"), "Gin 控制面接口监听地址")
	dbPath := fs.String("db", envOrDefault("EVALREEL_DB_PATH", "./evalreel.db"), "本地模式 SQLite 数据库文件路径")
	mysqlDSN := fs.String("mysql-dsn", envOrDefault("EVALREEL_MYSQL_DSN", ""), "服务端模式 MySQL DSN")
	redisAddr := fs.String("redis-addr", envOrDefault("EVALREEL_REDIS_ADDR", "127.0.0.1:6379"), "服务端模式 Redis 地址")
	redisPass := fs.String("redis-password", envOrDefault("EVALREEL_REDIS_PASSWORD", ""), "服务端模式 Redis 密码")
	redisDB := fs.Int("redis-db", 0, "服务端模式 Redis 数据库编号")
	logFormat := fs.String("log-format", envOrDefault("EVALREEL_LOG_FORMAT", "text"), "日志格式: text|json")
	logLevel := fs.String("log-level", envOrDefault("EVALREEL_LOG_LEVEL", "info"), "日志级别: debug|info|warn|error")
	otelExporter := fs.String("otel-exporter", envOrDefault("EVALREEL_OTEL_EXPORTER", "none"), "OpenTelemetry 导出: none|stdout|otlp")
	otelEndpoint := fs.String("otel-endpoint", envOrDefault("EVALREEL_OTEL_ENDPOINT", ""), "OTLP HTTP 收集端地址")
	_ = fs.Parse(args)

	logger := observability.NewLogger(observability.LogConfig{
		Service: "evalreel-server",
		Format:  *logFormat,
		Level:   *logLevel,
	}, os.Stdout)
	otelShutdown, otelErr := observability.InitTracer(context.Background(), observability.OTelConfig{
		ServiceName: "evalreel-server",
		Exporter:    *otelExporter,
		Endpoint:    *otelEndpoint,
	})
	if otelErr != nil {
		logger.Error("init otel tracer failed", "err", otelErr)
		os.Exit(1)
	}

	var (
		st         store.Store
		bus        scheduler.ProgressBus
		launcher   api.RunLauncher
		dispatcher *scheduler.AsynqDispatcher
		redisCli   *redis.Client
		err        error
	)

	switch *mode {
	case "local":
		st, err = store.NewSQLiteStore(*dbPath)
		if err != nil {
			logger.Error("open sqlite failed", "err", err)
			os.Exit(1)
		}
		bus = scheduler.NewProgressBroker()
		launcher = func(ctx context.Context, runID string, cfg *config.EvalConfig) {
			if err := executeEvalRun(ctx, st, bus.(*scheduler.ProgressBroker), runID, cfg, logger); err != nil {
				logger.Warn("background eval run stopped", "run_id", runID, "err", err)
			}
		}
	case "server":
		if *mysqlDSN == "" {
			logger.Error("server mode requires -mysql-dsn or EVALREEL_MYSQL_DSN")
			os.Exit(1)
		}
		st, err = store.NewMySQLStore(*mysqlDSN)
		if err != nil {
			logger.Error("open mysql failed", "err", err)
			os.Exit(1)
		}
		redisCli = redis.NewClient(&redis.Options{
			Addr:     *redisAddr,
			Password: *redisPass,
			DB:       *redisDB,
		})
		if err := redisCli.Ping(context.Background()).Err(); err != nil {
			logger.Error("ping redis failed", "err", err)
			_ = st.Close()
			_ = redisCli.Close()
			os.Exit(1)
		}
		bus = scheduler.NewRedisProgressBus(redisCli)
		dispatcher = scheduler.NewAsynqDispatcher(*redisAddr, *redisPass, *redisDB)
		launcher = func(ctx context.Context, runID string, cfg *config.EvalConfig) {
			tasks, cErr := st.ClaimPendingTasks(ctx, runID)
			if cErr != nil {
				logger.Error("load pending tasks for dispatch failed", "run_id", runID, "err", cErr)
				_ = st.UpdateRunStatus(context.Background(), runID, store.RunFailed)
				return
			}
			if dErr := dispatcher.DispatchRun(ctx, runID, tasks, *cfg); dErr != nil {
				logger.Error("dispatch tasks to asynq failed", "run_id", runID, "err", dErr)
				_ = st.UpdateRunStatus(context.Background(), runID, store.RunFailed)
			}
		}
	default:
		logger.Error("unsupported serve mode (want local|server)", "mode", *mode)
		os.Exit(1)
	}

	defer st.Close()
	defer bus.Close()
	if dispatcher != nil {
		defer dispatcher.Close()
	}
	if redisCli != nil {
		defer redisCli.Close()
	}

	apiSrv := api.NewServer(
		st,
		bus,
		func(_ context.Context, runID string, cfg *config.EvalConfig) ([]store.EvalTask, error) {
			return buildEvalTasks(runID, cfg)
		},
		launcher,
	)
	if dispatcher != nil {
		apiSrv.SetQueueStatsProvider(dispatcher.GetQueueStats)
	}

	srv := &http.Server{
		Addr:              *listen,
		Handler:           apiSrv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Info("evalreel api server started", "mode", *mode, "addr", *listen)
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
	_ = otelShutdown(sdCtx)
}

func envOrDefault(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
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
	spansByTrace := make(map[string][]store.TraceSpan)
	for _, tk := range tasks {
		if tk.TraceID == "" {
			continue
		}
		if _, seen := spansByTrace[tk.TraceID]; seen {
			continue
		}
		if sp, sErr := st.ListSpansByTraceID(ctx, tk.TraceID); sErr == nil && len(sp) > 0 {
			spansByTrace[tk.TraceID] = sp
		} else {
			spansByTrace[tk.TraceID] = nil
		}
	}
	summary := report.AggregateWithSpans(run, tasks, scores, spansByTrace)
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
