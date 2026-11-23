package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"evalreel/internal/config"
	"evalreel/internal/mockllm"
	"evalreel/internal/runner"
	"evalreel/internal/scheduler"
	"evalreel/internal/store"

	"github.com/alicebob/miniredis/v2"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func newWorkerTestEnv(t *testing.T, handler http.Handler) (*Processor, *store.SQLiteStore, *scheduler.RedisProgressBus, string) {
	t.Helper()
	st, err := store.NewSQLiteStore(filepath.Join(t.TempDir(), "worker_test.db"))
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	mr := miniredis.RunT(t)
	cli := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	lim := scheduler.NewRedisLuaLimiter(cli, nil)
	bus := scheduler.NewRedisProgressBus(cli)
	outDir := t.TempDir()
	proc := NewProcessor(st, lim, bus, 4, outDir, nil)

	mockSrv := httptest.NewServer(handler)
	t.Cleanup(func() {
		proc.Close()
		mockSrv.Client().CloseIdleConnections()
		mockSrv.Close()
		_ = bus.Close()
		_ = cli.Close()
		_ = st.Close()
	})
	return proc, st, bus, mockSrv.URL + "/v1"
}

func TestProcessor_ExecuteAndFinalizeReport(t *testing.T) {
	mockHandler := mockllm.NewServer(mockllm.Config{
		TTFT:          5 * time.Millisecond,
		ChunkInterval: 2 * time.Millisecond,
		ModelName:     "mock-model",
	}).Handler()
	proc, st, bus, baseURL := newWorkerTestEnv(t, mockHandler)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch, unsub, err := bus.SubscribeEvents(ctx, "run-w-1")
	if err != nil {
		t.Fatalf("SubscribeEvents: %v", err)
	}
	defer unsub()

	cfg := config.EvalConfig{
		Name:   "worker-e2e",
		Mode:   "benchmark",
		Trials: 1,
		Targets: []config.TargetConfig{{
			Name:    "mock-tgt",
			BaseURL: baseURL,
			Model:   "mock-model",
			RPM:     6000,
		}},
		Scorers: []config.ScorerConfig{{
			Type:    "rule",
			Name:    "contains_mock",
			Pattern: "模拟流式响应",
		}},
	}
	cfgBytes, _ := json.Marshal(cfg)

	const total = 3
	tasks := make([]store.EvalTask, total)
	for i := 0; i < total; i++ {
		snap, _ := json.Marshal(runner.DatasetItem{
			ID:     fmt.Sprintf("item-%d", i),
			Prompt: "hello",
		})
		tasks[i] = store.EvalTask{
			ID:              fmt.Sprintf("run-w-1-tk-%d", i),
			RunID:           "run-w-1",
			ItemID:          fmt.Sprintf("item-%d", i),
			TargetName:      "mock-tgt",
			TrialIndex:      0,
			Status:          store.TaskPending,
			RequestSnapshot: string(snap),
		}
	}

	if err := st.CreateRun(ctx, &store.EvalRun{
		ID:         "run-w-1",
		Name:       cfg.Name,
		Mode:       cfg.Mode,
		Status:     store.RunRunning,
		ConfigJSON: string(cfgBytes),
		TotalTasks: total,
	}, tasks); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	for _, tk := range tasks {
		pBytes, _ := json.Marshal(scheduler.EvalTaskPayload{RunID: tk.RunID, TaskID: tk.ID})
		if err := proc.ProcessTask(ctx, asynq.NewTask(scheduler.TaskTypeEval, pBytes)); err != nil {
			t.Fatalf("ProcessTask(%s): %v", tk.ID, err)
		}
	}

	gotRun, _, gotScores, err := st.GetRun(ctx, "run-w-1")
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Status != store.RunCompleted || gotRun.DoneTasks != total || len(gotScores) != total {
		t.Fatalf("unexpected run state: %+v scores=%d", gotRun, len(gotScores))
	}

	rpt, err := st.GetReport(ctx, "run-w-1")
	if err != nil || !strings.Contains(rpt.HTMLContent, "EvalReel 评测报告") {
		t.Fatalf("expected persisted report, err=%v", err)
	}

	var sawCompleted bool
	deadline := time.After(2 * time.Second)
	for !sawCompleted {
		select {
		case ev := <-ch:
			if ev.Status == store.RunCompleted && ev.Done == total {
				sawCompleted = true
			}
		case <-deadline:
			t.Fatalf("timed out waiting for RunCompleted event")
		}
	}
}

func TestProcessor_RetryableAndSkipRetry(t *testing.T) {
	// 返回 429 报错以验证 Processor 会保留重试资格而非跳过重试。
	h429 := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":{"message":"too many requests"}}`))
	})
	proc, st, _, baseURL := newWorkerTestEnv(t, h429)
	ctx := context.Background()

	cfg := config.EvalConfig{
		Name:    "worker-retry",
		Mode:    "benchmark",
		Targets: []config.TargetConfig{{Name: "m1", BaseURL: baseURL, Model: "m1"}},
	}
	cfgBytes, _ := json.Marshal(cfg)
	snap, _ := json.Marshal(runner.DatasetItem{ID: "q1", Prompt: "hi"})

	if err := st.CreateRun(ctx, &store.EvalRun{
		ID:         "run-w-err",
		Name:       cfg.Name,
		Mode:       cfg.Mode,
		Status:     store.RunRunning,
		ConfigJSON: string(cfgBytes),
		TotalTasks: 2,
	}, []store.EvalTask{
		{ID: "tk-429", RunID: "run-w-err", ItemID: "q1", TargetName: "m1", Status: store.TaskPending, RequestSnapshot: string(snap)},
		{ID: "tk-bad-json", RunID: "run-w-err", ItemID: "q2", TargetName: "m1", Status: store.TaskPending, RequestSnapshot: "{invalid"},
	}); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	// 1. 429 错误不得带 SkipRetry，应交由 Asynq 退避重投。
	p1, _ := json.Marshal(scheduler.EvalTaskPayload{RunID: "run-w-err", TaskID: "tk-429"})
	err := proc.ProcessTask(ctx, asynq.NewTask(scheduler.TaskTypeEval, p1))
	if err == nil || errors.Is(err, asynq.SkipRetry) {
		t.Fatalf("expected retryable error without SkipRetry, got %v", err)
	}
	if _, ok := scheduler.IsRetryable(err); !ok {
		t.Fatalf("expected RetryableError, got %v", err)
	}

	// 2. 损坏的数据快照属于不可恢复错误，应返回 asynq.SkipRetry 并直接落库 failed。
	p2, _ := json.Marshal(scheduler.EvalTaskPayload{RunID: "run-w-err", TaskID: "tk-bad-json"})
	err = proc.ProcessTask(ctx, asynq.NewTask(scheduler.TaskTypeEval, p2))
	if !errors.Is(err, asynq.SkipRetry) {
		t.Fatalf("expected asynq.SkipRetry for bad snapshot, got %v", err)
	}
	tk2, err := st.GetTask(ctx, "tk-bad-json")
	if err != nil || tk2.Status != store.TaskFailed {
		t.Fatalf("expected tk-bad-json failed, got %+v err=%v", tk2, err)
	}
}
