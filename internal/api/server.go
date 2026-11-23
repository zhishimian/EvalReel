package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"evalreel/internal/config"
	"evalreel/internal/observability"
	"evalreel/internal/report"
	"evalreel/internal/scheduler"
	"evalreel/internal/store"

	"github.com/gin-gonic/gin"
)

// TaskBuilder 根据评测配置拆分出需要落库与调度的子任务切片。
type TaskBuilder func(ctx context.Context, runID string, cfg *config.EvalConfig) ([]store.EvalTask, error)

// RunLauncher 在后台异步启动整场评测任务的调度与执行。
type RunLauncher func(ctx context.Context, runID string, cfg *config.EvalConfig)

// Server 提供基于 Gin 的评测控制接口与 SSE 实时进度推送服务。
type Server struct {
	engine       *gin.Engine
	store        store.Store
	broker       scheduler.ProgressBus
	builder      TaskBuilder
	launcher     RunLauncher
	queueStats   func(context.Context) (*scheduler.QueueStats, error)
	listArchived func(context.Context, int) ([]scheduler.ArchivedTaskInfo, error)
	retryArchive func(context.Context, []string, bool) (int, error)
	baseCtx      context.Context
	cancel       context.CancelFunc
	bgWG         sync.WaitGroup
	runSeq       atomic.Uint64
}

// NewServer 创建并挂载全部路由的 Gin API 服务实例。
func NewServer(st store.Store, broker scheduler.ProgressBus, builder TaskBuilder, launcher RunLauncher) *Server {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())
	r.GET("/metrics", gin.WrapH(observability.DefaultMetrics().Handler()))

	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{
		engine:   r,
		store:    st,
		broker:   broker,
		builder:  builder,
		launcher: launcher,
		baseCtx:  ctx,
		cancel:   cancel,
	}

	r.GET("/", s.handleUI)

	apiGroup := r.Group("/api")
	apiGroup.GET("/health", s.handleHealth)
	apiGroup.GET("/queues/stats", s.handleQueueStats)
	apiGroup.GET("/queues/archived", s.handleListArchived)
	apiGroup.POST("/queues/archived/retry", s.handleRetryArchived)
	apiGroup.POST("/runs", s.handleCreateRun)
	apiGroup.POST("/runs/:id/retry-failed", s.handleRetryFailed)
	apiGroup.GET("/runs", s.handleListRuns)
	apiGroup.GET("/runs/:id", s.handleGetRun)
	apiGroup.GET("/runs/:id/events", s.handleRunEvents)
	apiGroup.GET("/runs/:id/report", s.handleGetReport)
	apiGroup.GET("/compare", s.handleCompareRuns)

	return s
}

// SetQueueStatsProvider 注入分布式任务队列状态查询回调（服务端模式下由 AsynqDispatcher 提供）。
func (s *Server) SetQueueStatsProvider(fn func(context.Context) (*scheduler.QueueStats, error)) {
	s.queueStats = fn
}

// SetArchivedQueueProviders 注入死信队列查看与重放回调（服务端模式下使用）。
func (s *Server) SetArchivedQueueProviders(
	listFn func(context.Context, int) ([]scheduler.ArchivedTaskInfo, error),
	retryFn func(context.Context, []string, bool) (int, error),
) {
	s.listArchived = listFn
	s.retryArchive = retryFn
}

// Handler 返回标准 http.Handler 供 http.Server 或 httptest 挂载。
func (s *Server) Handler() http.Handler {
	return s.engine
}

// Shutdown 取消所有后台评测协程并等待其安全退出。
func (s *Server) Shutdown() {
	s.cancel()
	s.bgWG.Wait()
}

func (s *Server) handleHealth(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

func (s *Server) handleQueueStats(c *gin.Context) {
	if s.queueStats == nil {
		c.JSON(http.StatusOK, scheduler.QueueStats{Queue: "local"})
		return
	}
	stats, err := s.queueStats(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, stats)
}

func (s *Server) handleCreateRun(c *gin.Context) {
	var cfg config.EvalConfig
	if err := c.ShouldBindJSON(&cfg); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid json config: " + err.Error()})
		return
	}
	cfg.ApplyDefaults()
	if err := cfg.Validate(); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	idemKey := strings.TrimSpace(c.GetHeader("Idempotency-Key"))
	if idemKey != "" {
		existing, err := s.store.FindRunByIdempotencyKey(c.Request.Context(), idemKey)
		if err == nil {
			c.JSON(http.StatusOK, gin.H{
				"run_id":            existing.ID,
				"status":            existing.Status,
				"total_tasks":       existing.TotalTasks,
				"idempotent_replay": true,
			})
			return
		}
		if !errors.Is(err, store.ErrNotFound) {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "check idempotency key: " + err.Error()})
			return
		}
	}

	runID := fmt.Sprintf("run_%d_%d", time.Now().UnixMilli(), s.runSeq.Add(1))
	var tasks []store.EvalTask
	var err error
	if s.builder != nil {
		tasks, err = s.builder(c.Request.Context(), runID, &cfg)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "build tasks: " + err.Error()})
			return
		}
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
	if idemKey != "" {
		run.IdempotencyKey = &idemKey
	}

	if err := s.store.CreateRun(c.Request.Context(), run, tasks); err != nil {
		if errors.Is(err, store.ErrDuplicateKey) && idemKey != "" {
			if existing, fErr := s.store.FindRunByIdempotencyKey(c.Request.Context(), idemKey); fErr == nil {
				c.JSON(http.StatusOK, gin.H{
					"run_id":            existing.ID,
					"status":            existing.Status,
					"total_tasks":       existing.TotalTasks,
					"idempotent_replay": true,
				})
				return
			}
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "save run: " + err.Error()})
		return
	}

	if s.launcher != nil {
		cfgCopy := cfg
		s.bgWG.Add(1)
		go func() {
			defer s.bgWG.Done()
			s.launcher(s.baseCtx, runID, &cfgCopy)
		}()
	}

	c.JSON(http.StatusAccepted, gin.H{
		"run_id":      runID,
		"status":      run.Status,
		"total_tasks": run.TotalTasks,
	})
}

func (s *Server) handleListRuns(c *gin.Context) {
	limit := 20
	if raw := c.Query("limit"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			limit = parsed
		}
	}
	runs, err := s.store.ListRuns(c.Request.Context(), limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"runs": runs})
}

func (s *Server) handleGetRun(c *gin.Context) {
	runID := c.Param("id")
	run, tasks, scores, err := s.store.GetRun(c.Request.Context(), runID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "run not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"run":    run,
		"tasks":  tasks,
		"scores": scores,
	})
}

func (s *Server) handleRunEvents(c *gin.Context) {
	runID := c.Param("id")

	var ch <-chan scheduler.ProgressEvent
	var unsub func()
	if s.broker != nil {
		var subErr error
		ch, unsub, subErr = s.broker.SubscribeEvents(c.Request.Context(), runID)
		if subErr != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "subscribe events: " + subErr.Error()})
			return
		}
		defer unsub()
	}

	run, _, _, err := s.store.GetRun(c.Request.Context(), runID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "run not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")

	initialSent := false
	c.Stream(func(_ io.Writer) bool {
		if !initialSent {
			initialSent = true
			snap := scheduler.ProgressEvent{
				RunID:     run.ID,
				Total:     run.TotalTasks,
				Done:      run.DoneTasks,
				Failed:    run.FailedTasks,
				Status:    run.Status,
				Timestamp: time.Now().UTC(),
			}
			c.SSEvent("progress", snap)
			c.Writer.Flush()
			if isTerminalStatus(run.Status) || ch == nil {
				return false
			}
			return true
		}

		select {
		case ev, ok := <-ch:
			if !ok {
				return false
			}
			c.SSEvent("progress", ev)
			c.Writer.Flush()
			if isTerminalStatus(ev.Status) {
				return false
			}
			return true
		case <-c.Request.Context().Done():
			return false
		case <-s.baseCtx.Done():
			return false
		}
	})
}

func (s *Server) handleGetReport(c *gin.Context) {
	runID := c.Param("id")
	report, err := s.store.GetReport(c.Request.Context(), runID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "report not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.Data(http.StatusOK, "text/html; charset=utf-8", []byte(report.HTMLContent))
}

func isTerminalStatus(st store.RunStatus) bool {
	return st == store.RunCompleted || st == store.RunFailed || st == store.RunCanceled
}

func (s *Server) handleRetryFailed(c *gin.Context) {
	runID := c.Param("id")
	run, err := s.store.GetRunMeta(c.Request.Context(), runID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "run not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	retried, err := s.store.ResetFailedTasks(c.Request.Context(), runID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "reset failed tasks: " + err.Error()})
		return
	}
	if retried > 0 && s.launcher != nil {
		var cfg config.EvalConfig
		if uErr := json.Unmarshal([]byte(run.ConfigJSON), &cfg); uErr == nil {
			cfg.ApplyDefaults()
			s.bgWG.Add(1)
			go func() {
				defer s.bgWG.Done()
				s.launcher(s.baseCtx, runID, &cfg)
			}()
		}
	}

	c.JSON(http.StatusAccepted, gin.H{
		"run_id":        runID,
		"retried_tasks": retried,
	})
}

func (s *Server) handleListArchived(c *gin.Context) {
	if s.listArchived == nil {
		c.JSON(http.StatusOK, gin.H{"tasks": []any{}})
		return
	}
	limit := 50
	if raw := c.Query("limit"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			limit = parsed
		}
	}
	tasks, err := s.listArchived(c.Request.Context(), limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"tasks": tasks})
}

func (s *Server) handleRetryArchived(c *gin.Context) {
	if s.retryArchive == nil {
		c.JSON(http.StatusOK, gin.H{"retried_count": 0})
		return
	}
	var req struct {
		TaskIDs []string `json:"task_ids"`
		All     bool     `json:"all"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid json: " + err.Error()})
		return
	}
	n, err := s.retryArchive(c.Request.Context(), req.TaskIDs, req.All)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"retried_count": n})
}

func (s *Server) handleCompareRuns(c *gin.Context) {
	rawRuns := strings.TrimSpace(c.Query("runs"))
	if rawRuns == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "query parameter runs=id1,id2 is required"})
		return
	}
	var runIDs []string
	for _, part := range strings.Split(rawRuns, ",") {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			runIDs = append(runIDs, trimmed)
		}
	}
	if len(runIDs) < 2 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "at least 2 run_ids are required for comparison"})
		return
	}

	bundles := make([]report.RunBundle, 0, len(runIDs))
	for _, id := range runIDs {
		run, tasks, scores, err := s.store.GetRun(c.Request.Context(), id)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				c.JSON(http.StatusNotFound, gin.H{"error": "run not found: " + id})
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		bundles = append(bundles, report.RunBundle{Run: run, Tasks: tasks, Scores: scores})
	}

	targetFilter := strings.TrimSpace(c.Query("target"))
	comp := report.CompareRuns(bundles, targetFilter)
	if strings.EqualFold(strings.TrimSpace(c.Query("format")), "json") {
		c.JSON(http.StatusOK, comp)
		return
	}
	htmlStr, err := report.RenderCompareHTML(comp)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.Data(http.StatusOK, "text/html; charset=utf-8", []byte(htmlStr))
}
