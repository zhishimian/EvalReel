package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"evalreel/internal/chaos"
	"evalreel/internal/config"
	"evalreel/internal/mockllm"
	"evalreel/internal/observability"
	"evalreel/internal/proxy"
	"evalreel/internal/recorder"
	"evalreel/internal/sse"
	"evalreel/internal/store"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "mockllm":
		runMockLLM(os.Args[2:])
	case "proxy":
		runProxy(os.Args[2:])
	case "eval":
		runEval(os.Args[2:])
	case "serve":
		runServe(os.Args[2:])
	case "worker":
		runWorker(os.Args[2:])
	case "mcp-proxy":
		runMCPProxy(os.Args[2:])
	case "retry":
		runRetry(os.Args[2:])
	default:
		printUsage()
		os.Exit(1)
	}
}

func runMockLLM(args []string) {
	fs := flag.NewFlagSet("mockllm", flag.ExitOnError)
	listen := fs.String("listen", "127.0.0.1:9090", "模拟大模型服务的监听地址")
	ttft := fs.Duration("ttft", 300*time.Millisecond, "首字等待时间")
	interval := fs.Duration("interval", 30*time.Millisecond, "每块流式输出的时间间隔")
	chunks := fs.Int("chunks", 0, "正文流式输出块数（>0 时用于构造真实长流式负载）")
	fault429Rate := fs.Float64("fault-429-rate", 0, "随机注入 HTTP 429 的概率（0.0~1.0）")
	modelName := fs.String("model", "mock-reasoning-v1", "默认返回的模型名")
	_ = fs.Parse(args)

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	cfg := mockllm.Config{
		TTFT:          *ttft,
		ChunkInterval: *interval,
		Chunks:        *chunks,
		Fault429Rate:  *fault429Rate,
		ModelName:     *modelName,
		OnCanceled: func() {
			logger.Info("upstream generation canceled by client")
		},
	}

	srv := &http.Server{
		Addr:              *listen,
		Handler:           mockllm.NewServer(cfg).Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Info("mockllm server started", "addr", *listen, "ttft", ttft.String(), "interval", interval.String())
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("mockllm server failed", "err", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down mockllm server")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("mockllm shutdown error", "err", err)
	}
}

func runProxy(args []string) {
	loadDotEnv(".env")
	fs := flag.NewFlagSet("proxy", flag.ExitOnError)
	cfgPath := fs.String("config", "", "YAML 配置文件路径（留空则使用默认配置）")
	listenOverride := fs.String("listen", "", "覆盖代理监听地址")
	upstreamOverride := fs.String("upstream", "", "覆盖上游大模型地址")
	outDirOverride := fs.String("out", "", "覆盖轨迹输出目录")
	dbPath := fs.String("db", envOrDefault("EVALREEL_DB_PATH", ""), "可选 SQLite 路径（填写后同步写入 trace_spans 表）")
	mysqlDSN := fs.String("mysql-dsn", envOrDefault("EVALREEL_MYSQL_DSN", ""), "可选 MySQL DSN（填写后同步写入 trace_spans 表）")
	logFormat := fs.String("log-format", envOrDefault("EVALREEL_LOG_FORMAT", "text"), "日志格式: text|json")
	logLevel := fs.String("log-level", envOrDefault("EVALREEL_LOG_LEVEL", "info"), "日志级别: debug|info|warn|error")
	otelExporter := fs.String("otel-exporter", envOrDefault("EVALREEL_OTEL_EXPORTER", "none"), "OpenTelemetry 导出: none|stdout|otlp")
	otelEndpoint := fs.String("otel-endpoint", envOrDefault("EVALREEL_OTEL_ENDPOINT", ""), "OTLP HTTP 收集端地址")
	_ = fs.Parse(args)

	logger := observability.NewLogger(observability.LogConfig{
		Service: "evalreel-proxy",
		Format:  *logFormat,
		Level:   *logLevel,
	}, os.Stdout)
	otelShutdown, otelErr := observability.InitTracer(context.Background(), observability.OTelConfig{
		ServiceName: "evalreel-proxy",
		Exporter:    *otelExporter,
		Endpoint:    *otelEndpoint,
	})
	if otelErr != nil {
		logger.Error("init otel tracer failed", "err", otelErr)
		os.Exit(1)
	}

	var cfg *config.Config
	var err error
	if *cfgPath != "" {
		cfg, err = config.Load(*cfgPath)
		if err != nil {
			logger.Error("load config failed", "err", err)
			os.Exit(1)
		}
	} else {
		cfg = config.DefaultConfig()
	}

	if *listenOverride != "" {
		cfg.Server.Listen = *listenOverride
	}
	if *upstreamOverride != "" {
		cfg.Upstream.BaseURL = *upstreamOverride
	}
	if *outDirOverride != "" {
		cfg.Recorder.OutputDir = *outDirOverride
	}

	sessionID := cfg.Recorder.SessionID
	if sessionID == "" {
		sessionID = time.Now().Format("session_20060102_150405")
	}
	traceFile := filepath.Join(cfg.Recorder.OutputDir, sessionID+".jsonl.gz")

	rec, err := recorder.NewGzipJSONLRecorder(traceFile)
	if err != nil {
		logger.Error("init recorder failed", "err", err)
		os.Exit(1)
	}

	upstreamTimeout, err := cfg.ParseUpstreamTimeout()
	if err != nil {
		logger.Error("invalid upstream timeout", "err", err)
		os.Exit(1)
	}
	shutdownTimeout, err := cfg.ParseShutdownTimeout()
	if err != nil {
		logger.Error("invalid shutdown timeout", "err", err)
		os.Exit(1)
	}

	var spanStore store.Store
	if *mysqlDSN != "" {
		spanStore, err = store.NewMySQLStore(*mysqlDSN)
		if err != nil {
			logger.Error("open mysql span store failed", "err", err)
			_ = rec.Close()
			os.Exit(1)
		}
		defer spanStore.Close()
	} else if *dbPath != "" {
		spanStore, err = store.NewSQLiteStore(*dbPath)
		if err != nil {
			logger.Error("open sqlite span store failed", "err", err)
			_ = rec.Close()
			os.Exit(1)
		}
		defer spanStore.Close()
	}

	chaosEngine := chaos.NewEngine(cfg.Chaos)
	p, err := proxy.New(proxy.Options{
		UpstreamURL:      cfg.Upstream.BaseURL,
		UpstreamTimeout:  upstreamTimeout,
		SessionID:        sessionID,
		MaxRequestBodyMB: cfg.Recorder.MaxRequestBodyMB,
		Recorder:         rec,
		SpanStore:        spanStore,
		Chaos:            chaosEngine,
		EstimateTokens:   sse.NewTiktokenEstimator(),
		Logger:           logger,
	})
	if err != nil {
		logger.Error("init proxy failed", "err", err)
		_ = rec.Close()
		os.Exit(1)
	}

	var pprofSrv *http.Server
	if cfg.Server.PProfListen != "" {
		pprofSrv = &http.Server{
			Addr:              cfg.Server.PProfListen,
			Handler:           http.DefaultServeMux,
			ReadHeaderTimeout: 5 * time.Second,
		}
		go func() {
			logger.Info("pprof debug server started", "addr", cfg.Server.PProfListen)
			if err := pprofSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logger.Warn("pprof server stopped", "err", err)
			}
		}()
	}

	srv := &http.Server{
		Addr:              cfg.Server.Listen,
		Handler:           p.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Info("evalreel proxy started",
			"listen", cfg.Server.Listen,
			"upstream", cfg.Upstream.BaseURL,
			"trace_file", traceFile,
			"chaos_enabled", cfg.Chaos.Enabled,
		)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("proxy server failed", "err", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down proxy gracefully")

	sdCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(sdCtx); err != nil {
		logger.Error("proxy shutdown error", "err", err)
	}
	if pprofSrv != nil {
		_ = pprofSrv.Shutdown(sdCtx)
	}
	p.CloseIdleConnections()
	if err := rec.Close(); err != nil {
		logger.Error("close recorder error", "err", err)
	} else {
		logger.Info("trace file finalized", "path", traceFile)
	}
	_ = otelShutdown(sdCtx)
}

func printUsage() {
	fmt.Println("EvalReel CLI")
	fmt.Println("用法: evalreel <子命令> [参数]")
	fmt.Println("可用子命令:")
	fmt.Println("  mockllm   启动本地模拟 OpenAI 流式服务")
	fmt.Println("  proxy     启动透明流式反向代理与轨迹录制服务")
	fmt.Println("  eval      读取 JSON 配置批量运行评测并生成 report.html")
	fmt.Println("  serve     启动 Gin 评测控制面 API（支持 local 与 server 双模式）")
	fmt.Println("  worker     启动分布式评测 Worker 节点消费 Redis/Asynq 任务")
	fmt.Println("  mcp-proxy  启动 MCP 工具透明代理、只读 Mock 与混沌故障注入服务")
	fmt.Println("  retry      一键重跑指定评测场次中失败的子任务")
}
