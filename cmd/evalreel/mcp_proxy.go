package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"evalreel/internal/mcpproxy"
	"evalreel/internal/observability"
	"evalreel/internal/store"
)

func runMCPProxy(args []string) {
	loadDotEnv(".env")
	fs := flag.NewFlagSet("mcp-proxy", flag.ExitOnError)
	transport := fs.String("transport", "http", "传输方式: stdio | http")
	mode := fs.String("mode", "record", "代理模式: record | mock | chaos")
	listen := fs.String("listen", "127.0.0.1:8082", "HTTP 传输模式下的监听地址")
	upstreamURL := fs.String("upstream", "", "HTTP 传输模式下的上游 MCP 端点 URL")
	tapePath := fs.String("tape", "./traces/mcp_tape.mcp.jsonl.gz", "录制或回放使用的 .mcp.jsonl.gz 磁带路径")
	mockFallback := fs.String("mock-fallback", "strict", "Mock 未命中策略: strict | passthrough")
	chaosJSON := fs.String("chaos-rules", "", "工具侧混沌规则 JSON 文件路径")
	dbPath := fs.String("db", envOrDefault("EVALREEL_DB_PATH", ""), "可选 SQLite 路径（同步记录 mcp_tool Span）")
	mysqlDSN := fs.String("mysql-dsn", envOrDefault("EVALREEL_MYSQL_DSN", ""), "可选 MySQL DSN（同步记录 mcp_tool Span）")
	runID := fs.String("run-id", envOrDefault("EVALREEL_RUN_ID", ""), "关联评测 RunID")
	taskID := fs.String("task-id", envOrDefault("EVALREEL_TASK_ID", ""), "关联子任务 TaskID")
	logFormat := fs.String("log-format", envOrDefault("EVALREEL_LOG_FORMAT", "text"), "日志格式: text|json")
	_ = fs.Parse(args)

	// Stdio 模式下 stdout 必须保留给 JSON-RPC 报文，日志统一写到 stderr。
	logOut := os.Stdout
	if *transport == "stdio" {
		logOut = os.Stderr
	}
	logger := observability.NewLogger(observability.LogConfig{
		Service: "evalreel-mcp-proxy",
		Format:  *logFormat,
		Level:   "info",
	}, logOut)

	var (
		rec       *mcpproxy.TapeRecorder
		mockTape  *mcpproxy.TapeStore
		chaosEng  *mcpproxy.ChaosEngine
		spanStore store.Store
		err       error
	)

	if *mode == "mock" {
		mockTape, err = mcpproxy.LoadTape(*tapePath)
		if err != nil {
			logger.Error("load mcp mock tape failed", "path", *tapePath, "err", err)
			os.Exit(1)
		}
	} else if *tapePath != "" {
		rec, err = mcpproxy.NewTapeRecorder(*tapePath)
		if err != nil {
			logger.Error("open mcp tape recorder failed", "path", *tapePath, "err", err)
			os.Exit(1)
		}
		defer rec.Close()
	}

	if *chaosJSON != "" {
		raw, rErr := os.ReadFile(*chaosJSON)
		if rErr != nil {
			logger.Error("read mcp chaos rules failed", "err", rErr)
			os.Exit(1)
		}
		var rules []mcpproxy.ChaosRule
		if uErr := json.Unmarshal(raw, &rules); uErr != nil {
			logger.Error("parse mcp chaos rules failed", "err", uErr)
			os.Exit(1)
		}
		chaosEng = mcpproxy.NewChaosEngine(rules)
	}

	if *mysqlDSN != "" {
		spanStore, err = store.NewMySQLStore(*mysqlDSN)
		if err != nil {
			logger.Error("open mysql span store failed", "err", err)
			os.Exit(1)
		}
		defer spanStore.Close()
	} else if *dbPath != "" {
		spanStore, err = store.NewSQLiteStore(*dbPath)
		if err != nil {
			logger.Error("open sqlite span store failed", "err", err)
			os.Exit(1)
		}
		defer spanStore.Close()
	}

	interceptor := mcpproxy.NewInterceptor(mcpproxy.Options{
		Mode:         *mode,
		MockFallback: *mockFallback,
		Recorder:     rec,
		MockTape:     mockTape,
		Chaos:        chaosEng,
		SpanStore:    spanStore,
		RunID:        *runID,
		TaskID:       *taskID,
		Logger:       logger,
	})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	switch *transport {
	case "stdio":
		upArgs := fs.Args()
		var upCmd string
		var rest []string
		if len(upArgs) > 0 {
			upCmd = upArgs[0]
			rest = upArgs[1:]
		}
		sp := mcpproxy.NewStdioProxy(interceptor, upCmd, rest, os.Getenv("TRACEPARENT"))
		if err := sp.ServeStreams(ctx, os.Stdin, os.Stdout); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("stdio mcp proxy stopped with error", "err", err)
			os.Exit(1)
		}
	case "http":
		hp := mcpproxy.NewHTTPProxy(interceptor, *upstreamURL, 60*time.Second)
		defer hp.CloseIdleConnections()
		srv := &http.Server{
			Addr:              *listen,
			Handler:           hp,
			ReadHeaderTimeout: 10 * time.Second,
		}
		go func() {
			logger.Info("evalreel mcp http proxy started", "listen", *listen, "mode", *mode, "upstream", *upstreamURL)
			if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logger.Error("mcp http proxy failed", "err", err)
				os.Exit(1)
			}
		}()
		<-ctx.Done()
		sdCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(sdCtx)
	default:
		logger.Error("unsupported mcp transport (want stdio|http)", "transport", *transport)
		os.Exit(1)
	}
}
