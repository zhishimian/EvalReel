package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"evalreel/internal/config"
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
	engine   *gin.Engine
	store    store.Store
	broker   *scheduler.ProgressBroker
	builder  TaskBuilder
	launcher RunLauncher
	baseCtx  context.Context
	cancel   context.CancelFunc
	bgWG     sync.WaitGroup
	runSeq   atomic.Uint64
}

// NewServer 创建并挂载全部路由的 Gin API 服务实例。
func NewServer(st store.Store, broker *scheduler.ProgressBroker, builder TaskBuilder, launcher RunLauncher) *Server {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())

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

	apiGroup := r.Group("/api")
	apiGroup.GET("/health", s.handleHealth)
	apiGroup.POST("/runs", s.handleCreateRun)
	apiGroup.GET("/runs", s.handleListRuns)
	apiGroup.GET("/runs/:id", s.handleGetRun)
	apiGroup.GET("/runs/:id/events", s.handleRunEvents)
	apiGroup.GET("/runs/:id/report", s.handleGetReport)

	return s
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

	if err := s.store.CreateRun(c.Request.Context(), run, tasks); err != nil {
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
		ch, unsub = s.broker.Subscribe(runID)
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
