package observability

import (
	"io"
	"log/slog"
	"os"
	"strings"
)

// LogConfig 配置结构化日志的服务名、输出格式和最低级别。
type LogConfig struct {
	Service string
	Format  string // text | json
	Level   string // debug | info | warn | error
}

// NewLogger 创建支持文本与单行 JSON 两种格式的结构化 slog 日志器。
func NewLogger(cfg LogConfig, w io.Writer) *slog.Logger {
	if w == nil {
		w = os.Stdout
	}

	var level slog.Level
	switch strings.ToLower(strings.TrimSpace(cfg.Level)) {
	case "debug":
		level = slog.LevelDebug
	case "warn", "warning":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{Level: level}
	var handler slog.Handler
	if strings.EqualFold(strings.TrimSpace(cfg.Format), "json") {
		handler = slog.NewJSONHandler(w, opts)
	} else {
		handler = slog.NewTextHandler(w, opts)
	}

	logger := slog.New(handler)
	if svc := strings.TrimSpace(cfg.Service); svc != "" {
		logger = logger.With("service", svc)
	}
	return logger
}
