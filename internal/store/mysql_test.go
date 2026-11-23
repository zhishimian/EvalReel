package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func openTestMySQLStore(t *testing.T) (*MySQLStore, string) {
	t.Helper()
	dsn := strings.TrimSpace(os.Getenv("EVALREEL_TEST_MYSQL_DSN"))
	if dsn == "" {
		t.Skip("EVALREEL_TEST_MYSQL_DSN not set")
	}
	st, err := NewMySQLStore(dsn)
	if err != nil {
		t.Fatalf("NewMySQLStore failed: %v", err)
	}
	prefix := fmt.Sprintf("mytest_%d_", time.Now().UnixNano())
	t.Cleanup(func() {
		like := prefix + "%"
		_ = st.db.Where("trace_id LIKE ?", like).Delete(&TraceSpan{}).Error
		_ = st.db.Where("run_id LIKE ?", like).Delete(&EvalScore{}).Error
		_ = st.db.Where("run_id LIKE ?", like).Delete(&EvalReport{}).Error
		_ = st.db.Where("run_id LIKE ?", like).Delete(&EvalTask{}).Error
		_ = st.db.Where("id LIKE ?", like).Delete(&EvalRun{}).Error
		if err := st.Close(); err != nil {
			t.Errorf("close mysql store: %v", err)
		}
	})
	return st, prefix
}

func TestMySQLStore_LongTextAndResume(t *testing.T) {
	st, prefix := openTestMySQLStore(t)
	ctx := context.Background()

	runID := prefix + "long"
	tasks := []EvalTask{
		{ID: prefix + "tk0", RunID: runID, ItemID: "i0", TargetName: "m1", TrialIndex: 0, Status: TaskPending},
		{ID: prefix + "tk1", RunID: runID, ItemID: "i1", TargetName: "m1", TrialIndex: 0, Status: TaskRunning},
		{ID: prefix + "tk2", RunID: runID, ItemID: "i2", TargetName: "m1", TrialIndex: 0, Status: TaskSuccess},
	}
	if err := st.CreateRun(ctx, &EvalRun{
		ID:         runID,
		Name:       "mysql longtext",
		Mode:       "benchmark",
		Status:     RunRunning,
		ConfigJSON: `{"name":"mysql longtext"}`,
		TotalTasks: len(tasks),
	}, tasks); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	pending, err := st.ClaimPendingTasks(ctx, runID)
	if err != nil || len(pending) != 2 {
		t.Fatalf("ClaimPendingTasks: len=%d err=%v", len(pending), err)
	}

	gotTk, err := st.GetTask(ctx, prefix+"tk0")
	if err != nil || gotTk.ItemID != "i0" {
		t.Fatalf("GetTask: tk=%+v err=%v", gotTk, err)
	}

	// 验证超过 MySQL TEXT 64KB 上限的大报告能够正常写入与读出。
	bigHTML := "<html><body>" + strings.Repeat("A", 90*1024) + "</body></html>"
	if err := st.SaveReport(ctx, &EvalReport{
		RunID:       runID,
		SummaryJSON: `{"ok":true}`,
		HTMLContent: bigHTML,
	}); err != nil {
		t.Fatalf("SaveReport >64KB: %v", err)
	}
	rpt, err := st.GetReport(ctx, runID)
	if err != nil || len(rpt.HTMLContent) != len(bigHTML) {
		t.Fatalf("GetReport length mismatch: got=%d want=%d err=%v", len(rpt.HTMLContent), len(bigHTML), err)
	}
}

func TestMySQLStore_IdempotencyAndNullKeys(t *testing.T) {
	st, prefix := openTestMySQLStore(t)
	ctx := context.Background()

	// 两条都不传幂等键（NULL）时必须都能写入。
	for i := 0; i < 2; i++ {
		if err := st.CreateRun(ctx, &EvalRun{
			ID:         fmt.Sprintf("%snull_%d", prefix, i),
			Mode:       "benchmark",
			Status:     RunRunning,
			ConfigJSON: `{}`,
			TotalTasks: 1,
		}, nil); err != nil {
			t.Fatalf("CreateRun NULL key %d: %v", i, err)
		}
	}

	idem := prefix + "idem"
	if err := st.CreateRun(ctx, &EvalRun{
		ID:             prefix + "run_1",
		IdempotencyKey: &idem,
		Mode:           "benchmark",
		Status:         RunRunning,
		ConfigJSON:     `{}`,
		TotalTasks:     1,
	}, nil); err != nil {
		t.Fatalf("CreateRun with idem key: %v", err)
	}

	err := st.CreateRun(ctx, &EvalRun{
		ID:             prefix + "run_2",
		IdempotencyKey: &idem,
		Mode:           "benchmark",
		Status:         RunRunning,
		ConfigJSON:     `{}`,
		TotalTasks:     1,
	}, nil)
	if !errors.Is(err, ErrDuplicateKey) {
		t.Fatalf("expected ErrDuplicateKey, got %v", err)
	}

	found, err := st.FindRunByIdempotencyKey(ctx, idem)
	if err != nil || found.ID != prefix+"run_1" {
		t.Fatalf("FindRunByIdempotencyKey: run=%+v err=%v", found, err)
	}
}

func TestMySQLStore_ConcurrentAndAtomicFinalize(t *testing.T) {
	st, prefix := openTestMySQLStore(t)
	ctx := context.Background()

	const total = 100
	runID := prefix + "conc"
	tasks := make([]EvalTask, total)
	for i := 0; i < total; i++ {
		tasks[i] = EvalTask{
			ID:         fmt.Sprintf("%stk_%03d", prefix, i),
			RunID:      runID,
			ItemID:     fmt.Sprintf("item_%02d", i%10),
			TargetName: "mock-model",
			TrialIndex: i / 10,
			Status:     TaskPending,
		}
	}
	if err := st.CreateRun(ctx, &EvalRun{
		ID:         runID,
		Mode:       "benchmark",
		Status:     RunRunning,
		ConfigJSON: `{}`,
		TotalTasks: total,
	}, tasks); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	ch := make(chan EvalTask, total)
	for _, tk := range tasks {
		ch <- tk
	}
	close(ch)

	const workers = 20
	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()
			for tk := range ch {
				_ = st.MarkTaskRunning(ctx, tk.ID, 0)
				status := TaskSuccess
				if tk.TrialIndex%5 == 4 {
					status = TaskFailed
				}
				if err := st.UpdateTaskResult(ctx, tk.ID, TaskResultUpdate{
					Status:       status,
					ResponseText: "ok",
					TotalMs:      100,
				}); err != nil {
					t.Errorf("UpdateTaskResult(%s): %v", tk.ID, err)
				}
			}
		}()
	}
	wg.Wait()

	// 重复更新同一个已完成子任务不会再次递增计数。
	if err := st.UpdateTaskResult(ctx, tasks[0].ID, TaskResultUpdate{Status: TaskSuccess}); err != nil {
		t.Fatalf("duplicate UpdateTaskResult: %v", err)
	}

	var finalizeWins atomic.Int32
	var fwg sync.WaitGroup
	fwg.Add(workers)
	for w := 0; w < workers; w++ {
		go func() {
			defer fwg.Done()
			won, err := st.TryFinalizeRun(ctx, runID, time.Now())
			if err != nil {
				t.Errorf("TryFinalizeRun: %v", err)
				return
			}
			if won {
				finalizeWins.Add(1)
			}
		}()
	}
	fwg.Wait()

	if finalizeWins.Load() != 1 {
		t.Fatalf("expected exactly 1 finalize winner across 20 workers, got %d", finalizeWins.Load())
	}

	gotRun, _, _, err := st.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.DoneTasks != total || gotRun.FailedTasks != 20 || gotRun.Status != RunCompleted {
		t.Fatalf("unexpected run state: %+v", gotRun)
	}
}

func TestMySQLStore_Spans(t *testing.T) {
	st, prefix := openTestMySQLStore(t)
	ctx := context.Background()

	traceID := prefix + "trace"
	spans := []TraceSpan{
		{SpanID: "sp_child", ParentSpanID: "sp_root", TraceID: traceID, AgentName: "coder", StartTimeMs: 250, DurationMs: 120, Status: "ok"},
		{SpanID: "sp_root", TraceID: traceID, AgentName: "planner", StartTimeMs: 100, DurationMs: 300, Status: "ok"},
	}
	if err := st.SaveSpans(ctx, spans); err != nil {
		t.Fatalf("SaveSpans: %v", err)
	}
	got, err := st.ListSpansByTraceID(ctx, traceID)
	if err != nil || len(got) != 2 || got[0].SpanID != "sp_root" || got[1].SpanID != "sp_child" {
		t.Fatalf("ListSpansByTraceID: got=%+v err=%v", got, err)
	}
}

func TestMySQLStore_ResetFailedTasks(t *testing.T) {
	st, prefix := openTestMySQLStore(t)
	ctx := context.Background()

	runID := prefix + "rf"
	tasks := []EvalTask{
		{ID: prefix + "rf0", RunID: runID, ItemID: "i0", TargetName: "m1", Status: TaskPending},
		{ID: prefix + "rf1", RunID: runID, ItemID: "i1", TargetName: "m1", Status: TaskPending},
	}
	if err := st.CreateRun(ctx, &EvalRun{
		ID:         runID,
		Mode:       "benchmark",
		Status:     RunRunning,
		ConfigJSON: `{}`,
		TotalTasks: 2,
	}, tasks); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	_ = st.UpdateTaskResult(ctx, tasks[0].ID, TaskResultUpdate{Status: TaskSuccess})
	_ = st.UpdateTaskResult(ctx, tasks[1].ID, TaskResultUpdate{Status: TaskFailed, ErrorMsg: "timeout"})
	_, _ = st.TryFinalizeRun(ctx, runID, time.Now())

	retried, err := st.ResetFailedTasks(ctx, runID)
	if err != nil || retried != 1 {
		t.Fatalf("ResetFailedTasks: retried=%d err=%v", retried, err)
	}
	meta, err := st.GetRunMeta(ctx, runID)
	if err != nil || meta.Status != RunRunning || meta.DoneTasks != 1 || meta.FailedTasks != 0 {
		t.Fatalf("unexpected mysql run state after reset: %+v err=%v", meta, err)
	}
}
