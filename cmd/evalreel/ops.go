package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"evalreel/internal/config"
	"evalreel/internal/observability"
	"evalreel/internal/scheduler"
	"evalreel/internal/store"
)

func runRetry(args []string) {
	loadDotEnv(".env")
	fs := flag.NewFlagSet("retry", flag.ExitOnError)
	runID := fs.String("run", "", "需要重跑失败子任务的 run_id")
	dbPath := fs.String("db", envOrDefault("EVALREEL_DB_PATH", "./evalreel.db"), "本地模式 SQLite 数据库文件路径")
	mysqlDSN := fs.String("mysql-dsn", envOrDefault("EVALREEL_MYSQL_DSN", ""), "服务端模式 MySQL DSN（填写后切换集群重投）")
	redisAddr := fs.String("redis-addr", envOrDefault("EVALREEL_REDIS_ADDR", "127.0.0.1:6379"), "服务端模式 Redis 地址")
	redisPass := fs.String("redis-password", envOrDefault("EVALREEL_REDIS_PASSWORD", ""), "服务端模式 Redis 密码")
	_ = fs.Parse(args)

	logger := observability.NewLogger(observability.LogConfig{
		Service: "evalreel-retry",
		Format:  "text",
		Level:   "info",
	}, os.Stdout)

	if *runID == "" {
		logger.Error("retry requires -run <run_id>")
		os.Exit(1)
	}

	var (
		st  store.Store
		err error
	)
	if *mysqlDSN != "" {
		st, err = store.NewMySQLStore(*mysqlDSN)
	} else {
		st, err = store.NewSQLiteStore(*dbPath)
	}
	if err != nil {
		logger.Error("open store failed", "err", err)
		os.Exit(1)
	}
	defer st.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	run, err := st.GetRunMeta(ctx, *runID)
	if err != nil {
		logger.Error("load run meta failed", "run_id", *runID, "err", err)
		os.Exit(1)
	}
	retried, err := st.ResetFailedTasks(ctx, *runID)
	if err != nil {
		logger.Error("reset failed tasks error", "run_id", *runID, "err", err)
		os.Exit(1)
	}
	if retried == 0 {
		logger.Info("no failed tasks to retry", "run_id", *runID)
		return
	}
	logger.Info("failed tasks reset to pending", "run_id", *runID, "retried_tasks", retried)

	var cfg config.EvalConfig
	if err := json.Unmarshal([]byte(run.ConfigJSON), &cfg); err != nil {
		logger.Error("parse stored run config failed", "err", err)
		os.Exit(1)
	}
	cfg.ApplyDefaults()

	if *mysqlDSN != "" {
		disp := scheduler.NewAsynqDispatcher(*redisAddr, *redisPass, 0)
		defer disp.Close()
		tasks, cErr := st.ClaimPendingTasks(ctx, *runID)
		if cErr != nil {
			logger.Error("claim reset tasks failed", "err", cErr)
			os.Exit(1)
		}
		if dErr := disp.RedispatchTasks(ctx, *runID, tasks, cfg); dErr != nil {
			logger.Error("redispatch tasks failed", "err", dErr)
			os.Exit(1)
		}
		fmt.Printf("已将 %d 条失败任务重新投递至分布式队列 (%s)\n", len(tasks), *runID)
		return
	}

	broker := scheduler.NewProgressBroker()
	if err := executeEvalRun(ctx, st, broker, *runID, &cfg, logger); err != nil {
		if errors.Is(err, context.Canceled) {
			logger.Warn("retry canceled; resume later with eval -resume", "run_id", *runID)
			return
		}
		logger.Error("retry eval run failed", "err", err)
		os.Exit(1)
	}
}
