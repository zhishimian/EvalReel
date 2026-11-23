package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

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
	_ = fs.Parse(args)

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	if *mysqlDSN == "" {
		logger.Error("worker requires -mysql-dsn or EVALREEL_MYSQL_DSN")
		os.Exit(1)
	}

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
}
