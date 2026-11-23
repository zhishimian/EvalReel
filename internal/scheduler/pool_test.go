package scheduler

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"evalreel/internal/store"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func newTestRun(t *testing.T, st store.Store, runID string, total int, target string) {
	t.Helper()
	run := &store.EvalRun{
		ID:         runID,
		Name:       runID,
		Mode:       "benchmark",
		Status:     store.RunRunning,
		ConfigJSON: `{}`,
		TotalTasks: total,
	}
	tasks := make([]store.EvalTask, total)
	for i := 0; i < total; i++ {
		tasks[i] = store.EvalTask{
			ID:         fmt.Sprintf("%s-t-%03d", runID, i),
			RunID:      runID,
			ItemID:     fmt.Sprintf("item-%02d", i%10),
			TargetName: target,
			TrialIndex: i / 10,
			Status:     store.TaskPending,
		}
	}
	if err := st.CreateRun(context.Background(), run, tasks); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
}

func openStore(t *testing.T) *store.SQLiteStore {
	t.Helper()
	st, err := store.NewSQLiteStore(filepath.Join(t.TempDir(), "sched.db"))
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	t.Cleanup(func() {
		if err := st.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})
	return st
}

func TestScheduler_WorkerPoolAndProgress(t *testing.T) {
	st := openStore(t)
	const total = 30
	newTestRun(t, st, "run-pool", total, "qwen3.8-flash")

	broker := NewProgressBroker()
	evCh, unsub := broker.Subscribe("run-pool")
	defer unsub()

	sched := New(Config{
		Concurrency: 5,
		MaxRetries:  1,
		TaskTimeout: 5 * time.Second,
		BaseBackoff: 5 * time.Millisecond,
	}, st, NewLocalLimiter(map[string]int{"qwen3.8-flash": 6000}), broker)

	var execCount atomic.Int64
	err := sched.Run(context.Background(), "run-pool", func(_ context.Context, tk store.EvalTask) (store.TaskResultUpdate, []store.EvalScore, error) {
		execCount.Add(1)
		time.Sleep(5 * time.Millisecond)
		return store.TaskResultUpdate{
				Status:       store.TaskSuccess,
				ResponseText: "ok-" + tk.ID,
				TotalMs:      5,
			}, []store.EvalScore{{
				TaskID:     tk.ID,
				RunID:      tk.RunID,
				ScorerName: "rule",
				Passed:     true,
			}}, nil
	})
	if err != nil {
		t.Fatalf("Scheduler.Run failed: %v", err)
	}

	if execCount.Load() != total {
		t.Fatalf("expected %d executions, got %d", total, execCount.Load())
	}

	gotRun, _, gotScores, err := st.GetRun(context.Background(), "run-pool")
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Status != store.RunCompleted || gotRun.DoneTasks != total || len(gotScores) != total {
		t.Fatalf("unexpected run state: %+v scores=%d", gotRun, len(gotScores))
	}

	var sawCompleted bool
	for {
		select {
		case ev := <-evCh:
			if ev.Status == store.RunCompleted && ev.Done == total {
				sawCompleted = true
			}
		default:
			if !sawCompleted {
				t.Fatalf("expected RunCompleted progress event")
			}
			return
		}
	}
}

func TestScheduler_PerTargetRateLimit(t *testing.T) {
	// 600 RPM = 10 requests/sec = 100ms per request when burst=1
	lim := NewLocalLimiterWithBurst(map[string]int{"slow-model": 600}, 1)
	ctx := context.Background()

	start := time.Now()
	for i := 0; i < 4; i++ {
		if err := lim.Wait(ctx, "slow-model"); err != nil {
			t.Fatalf("Wait(%d): %v", i, err)
		}
	}
	elapsed := time.Since(start)
	// 第 1 次立即放行，后续 3 次至少各间隔约 100ms（留少量调度余量检查 >= 240ms）
	if elapsed < 240*time.Millisecond {
		t.Fatalf("expected rate limit >= 240ms for 4 tokens at 600 RPM, got %v", elapsed)
	}
}

func TestScheduler_RetryAndBackoff(t *testing.T) {
	st := openStore(t)
	newTestRun(t, st, "run-retry", 2, "qwen3.8-flash")

	sched := New(Config{
		Concurrency: 2,
		MaxRetries:  2,
		TaskTimeout: 5 * time.Second,
		BaseBackoff: 10 * time.Millisecond,
	}, st, nil, nil)

	attempts := make(map[string]*atomic.Int64)
	attempts["run-retry-t-000"] = &atomic.Int64{}
	attempts["run-retry-t-001"] = &atomic.Int64{}

	err := sched.Run(context.Background(), "run-retry", func(_ context.Context, tk store.EvalTask) (store.TaskResultUpdate, []store.EvalScore, error) {
		n := attempts[tk.ID].Add(1)
		if tk.ID == "run-retry-t-000" {
			// 第 1、2 次报 429 可重试，第 3 次成功（RetryCount 应为 2）
			if n <= 2 {
				return store.TaskResultUpdate{}, nil, NewRetryableError(errors.New("429 rate limit"), 10*time.Millisecond)
			}
			return store.TaskResultUpdate{Status: store.TaskSuccess, ResponseText: "recovered"}, nil, nil
		}
		// 第 2 题始终报 429，耗尽重试次数后应标记 TaskFailed
		return store.TaskResultUpdate{}, nil, NewRetryableError(errors.New("persistent 429"), 5*time.Millisecond)
	})
	if err != nil {
		t.Fatalf("Scheduler.Run failed: %v", err)
	}

	gotRun, gotTasks, _, err := st.GetRun(context.Background(), "run-retry")
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.DoneTasks != 2 || gotRun.FailedTasks != 1 {
		t.Fatalf("unexpected run counters: %+v", gotRun)
	}
	if gotTasks[0].Status != store.TaskSuccess || gotTasks[0].RetryCount != 2 {
		t.Fatalf("task 0 expected success with RetryCount=2, got %+v", gotTasks[0])
	}
	if gotTasks[1].Status != store.TaskFailed || gotTasks[1].RetryCount != 2 {
		t.Fatalf("task 1 expected failed with RetryCount=2, got %+v", gotTasks[1])
	}
}

func TestScheduler_GracefulShutdownAndResume(t *testing.T) {
	st := openStore(t)
	const total = 20
	newTestRun(t, st, "run-resume", total, "qwen3.8-flash")

	sched := New(Config{
		Concurrency: 2,
		MaxRetries:  0,
		TaskTimeout: 5 * time.Second,
		BaseBackoff: 5 * time.Millisecond,
	}, st, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	var phase1Done atomic.Int64

	err := sched.Run(ctx, "run-resume", func(taskCtx context.Context, tk store.EvalTask) (store.TaskResultUpdate, []store.EvalScore, error) {
		select {
		case <-time.After(15 * time.Millisecond):
		case <-taskCtx.Done():
			return store.TaskResultUpdate{}, nil, taskCtx.Err()
		}
		if phase1Done.Add(1) >= 6 {
			cancel()
		}
		return store.TaskResultUpdate{Status: store.TaskSuccess, ResponseText: "ok"}, nil, nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled on shutdown, got %v", err)
	}

	midRun, _, _, err := st.GetRun(context.Background(), "run-resume")
	if err != nil {
		t.Fatalf("GetRun mid: %v", err)
	}
	if midRun.Status != store.RunCanceled || midRun.DoneTasks >= total {
		t.Fatalf("expected RunCanceled with partial progress < %d, got %+v", total, midRun)
	}

	// 断点续跑同一 runID：已完成任务不应重复执行，最终全部补齐
	var phase2Exec atomic.Int64
	if err := sched.Run(context.Background(), "run-resume", func(_ context.Context, tk store.EvalTask) (store.TaskResultUpdate, []store.EvalScore, error) {
		phase2Exec.Add(1)
		return store.TaskResultUpdate{Status: store.TaskSuccess, ResponseText: "resumed"}, nil, nil
	}); err != nil {
		t.Fatalf("resume Run failed: %v", err)
	}

	finalRun, _, _, err := st.GetRun(context.Background(), "run-resume")
	if err != nil {
		t.Fatalf("GetRun final: %v", err)
	}
	if finalRun.Status != store.RunCompleted || finalRun.DoneTasks != total {
		t.Fatalf("expected RunCompleted with DoneTasks=%d, got %+v", total, finalRun)
	}
	if int(phase2Exec.Load()) != total-midRun.DoneTasks {
		t.Fatalf("expected phase2 to execute exact remaining %d tasks, executed %d",
			total-midRun.DoneTasks, phase2Exec.Load())
	}
}
