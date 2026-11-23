package main

import (
	"context"
	"errors"
	"flag"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"evalreel/internal/observability"
	"evalreel/internal/scheduler"
	"evalreel/internal/store"
	"evalreel/internal/worker"

	"github.com/redis/go-redis/v9"
)

func runWorker(args []string) {
	loadDotEnv(".env")
	fs := flag.NewFlagSet("worker", flag.ExitOnError)
	mysqlDSN := fs.String("mysql-dsn", envOrDefault("EVALREEL_MYSQL_DSN", ""), "MySQL DSN")
	redisAddr := fs.String("redis-addr", envOrDefault("EVALREEL_REDIS_ADDR", "127.0.0.1:6379"), "Redis 地址")
	redisPass := fs.String("redis-password", envOrDefault("EVALREEL_REDIS_PASSWORD", ""), "Redis 密码")
	redisDB := fs.Int("redis-db", 0, "Redis 数据库编号")
	concurrency := fs.Int("concurrency", 10, "单 Worker 节点并发协程数")
	outputDir := fs.String("output-dir", envOrDefault("EVALREEL_OUTPUT_DIR", "./reports"), "HTML 战报输出目录")
	metricsListen := fs.String("metrics-listen", envOrDefault("EVALREEL_METRICS_LISTEN", ""), "可选监控 /metrics HTTP 监听地址")
	logFormat := fs.String("log-format", envOrDefault("EVALREEL_LOG_FORMAT", "text"), "日志格式: text|json")
	logLevel := fs.String("log-level", envOrDefault("EVALREEL_LOG_LEVEL", "info"), "日志级别: debug|info|warn|error")
	otelExporter := fs.String("otel-exporter", envOrDefault("EVALREEL_OTEL_EXPORTER", "none"), "OpenTelemetry 导出: none|stdout|otlp")
	otelEndpoint := fs.String("otel-endpoint", envOrDefault("EVALREEL_OTEL_ENDPOINT", ""), "OTLP HTTP 收集端地址")
	_ = fs.Parse(args)

	logger := observability.NewLogger(observability.LogConfig{
		Service: "evalreel-worker",
		Format:  *logFormat,
		Level:   *logLevel,
	}, os.Stdout)
	if *mysqlDSN == "" {
		logger.Error("worker requires -mysql-dsn or EVALREEL_MYSQL_DSN")
		os.Exit(1)
	}

	otelShutdown, otelErr := observability.InitTracer(context.Background(), observability.OTelConfig{
		ServiceName: "evalreel-worker",
		Exporter:    *otelExporter,
		Endpoint:    *otelEndpoint,
	})
	if otelErr != nil {
		logger.Error("init otel tracer failed", "err", otelErr)
		os.Exit(1)
	}
	defer func() {
		sdCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = otelShutdown(sdCtx)
	}()

	st, err := store.NewMySQLStore(*mysqlDSN)
	if err != nil {
		logger.Error("open mysql failed", "err", err)
		os.Exit(1)
	}
	defer st.Close()

	redisCli := redis.NewClient(&redis.Options{
		Addr:     *redisAddr,
		Password: *redisPass,
		DB:       *redisDB,
	})
	if err := redisCli.Ping(context.Background()).Err(); err != nil {
		logger.Error("ping redis failed", "err", err)
		_ = redisCli.Close()
		os.Exit(1)
	}
	defer redisCli.Close()

	var metricsSrv *http.Server
	if *metricsListen != "" {
		mux := http.NewServeMux()
		mux.Handle("/metrics", observability.DefaultMetrics().Handler())
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		})
		metricsSrv = &http.Server{
			Addr:              *metricsListen,
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
		}
		go func() {
			logger.Info("worker metrics server started", "addr", *metricsListen)
			if err := metricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logger.Warn("worker metrics server stopped", "err", err)
			}
		}()
	}

	lim := scheduler.NewRedisLuaLimiter(redisCli, nil)
	bus := scheduler.NewRedisProgressBus(redisCli)
	defer bus.Close()

	proc := worker.NewProcessor(st, lim, bus, *concurrency, *outputDir, logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := proc.RunServer(ctx, *redisAddr, *redisPass, *redisDB); err != nil {
		logger.Error("worker stopped with error", "err", err)
		os.Exit(1)
	}
	if metricsSrv != nil {
		sdCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = metricsSrv.Shutdown(sdCtx)
	}
}
