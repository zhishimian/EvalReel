package store

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
)

func openTestStore(t *testing.T) *SQLiteStore {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "evalreel_test.db")
	st, err := NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStore failed: %v", err)
	}
	t.Cleanup(func() {
		if err := st.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})
	return st
}

func TestSQLiteStore_CreateAndResume(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	const total = 100
	run := &EvalRun{
		ID:         "run-resume-1",
		Name:       "resume test",
		Mode:       "benchmark",
		Status:     RunRunning,
		ConfigJSON: `{"name":"resume test"}`,
		TotalTasks: total,
	}

	tasks := make([]EvalTask, total)
	for i := 0; i < total; i++ {
		tasks[i] = EvalTask{
			ID:         fmt.Sprintf("task-%03d", i),
			RunID:      run.ID,
			ItemID:     fmt.Sprintf("item-%02d", i%10),
			TargetName: "qwen3.8-flash",
			TrialIndex: i / 10,
			Status:     TaskPending,
		}
	}

	if err := st.CreateRun(ctx, run, tasks); err != nil {
		t.Fatalf("CreateRun failed: %v", err)
	}

	// 模拟先跑完前 60 条（成功），第 60~69 条停在 running 状态时突然断电崩溃
	for i := 0; i < 60; i++ {
		err := st.UpdateTaskResult(ctx, tasks[i].ID, TaskResultUpdate{
			Status:       TaskSuccess,
			ResponseText: "ok",
			TotalMs:      150,
			InputTokens:  20,
			OutputTokens: 30,
		})
		if err != nil {
			t.Fatalf("UpdateTaskResult(%d): %v", i, err)
		}
	}
	for i := 60; i < 70; i++ {
		if err := st.MarkTaskRunning(ctx, tasks[i].ID, 0); err != nil {
			t.Fatalf("MarkTaskRunning(%d): %v", i, err)
		}
	}

	// 重复更新同一条已完成任务不应重复增加 DoneTasks
	if err := st.UpdateTaskResult(ctx, tasks[0].ID, TaskResultUpdate{Status: TaskSuccess, ResponseText: "ok2"}); err != nil {
		t.Fatalf("idempotent UpdateTaskResult: %v", err)
	}

	pending, err := st.ClaimPendingTasks(ctx, run.ID)
	if err != nil {
		t.Fatalf("ClaimPendingTasks: %v", err)
	}
	if len(pending) != 40 {
		t.Fatalf("expected 40 unfinished tasks (10 running + 30 pending), got %d", len(pending))
	}

	gotRun, gotTasks, _, err := st.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.DoneTasks != 60 || gotRun.FailedTasks != 0 || len(gotTasks) != total {
		t.Fatalf("unexpected run state: %+v, tasks=%d", gotRun, len(gotTasks))
	}
}

func TestSQLiteStore_ConcurrentUpdates(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	const total = 100
	run := &EvalRun{
		ID:         "run-conc-1",
		Name:       "concurrent test",
		Mode:       "benchmark",
		Status:     RunRunning,
		ConfigJSON: `{}`,
		TotalTasks: total,
	}
	tasks := make([]EvalTask, total)
	for i := 0; i < total; i++ {
		tasks[i] = EvalTask{
			ID:         fmt.Sprintf("conc-task-%03d", i),
			RunID:      run.ID,
			ItemID:     fmt.Sprintf("q-%02d", i%10),
			TargetName: "deepseek-v4.1-flash-1017",
			TrialIndex: i / 10,
			Status:     TaskPending,
		}
	}
	if err := st.CreateRun(ctx, run, tasks); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	taskCh := make(chan EvalTask, total)
	for _, tk := range tasks {
		taskCh <- tk
	}
	close(taskCh)

	const workers = 20
	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()
			for tk := range taskCh {
				if err := st.MarkTaskRunning(ctx, tk.ID, 0); err != nil {
					t.Errorf("MarkTaskRunning(%s): %v", tk.ID, err)
					continue
				}
				status := TaskSuccess
				if tk.TrialIndex%5 == 4 {
					status = TaskFailed
				}
				if err := st.UpdateTaskResult(ctx, tk.ID, TaskResultUpdate{
					Status:       status,
					ResponseText: "answer",
					TotalMs:      220,
				}); err != nil {
					t.Errorf("UpdateTaskResult(%s): %v", tk.ID, err)
					continue
				}
				if err := st.SaveScores(ctx, []EvalScore{{
					TaskID:     tk.ID,
					RunID:      run.ID,
					ScorerName: "rule_tool",
					Passed:     status == TaskSuccess,
					Reason:     "checked",
				}}); err != nil {
					t.Errorf("SaveScores(%s): %v", tk.ID, err)
				}
			}
		}()
	}
	wg.Wait()

	gotRun, _, gotScores, err := st.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.DoneTasks != total {
		t.Fatalf("expected DoneTasks=%d, got %d", total, gotRun.DoneTasks)
	}
	if gotRun.FailedTasks != 20 {
		t.Fatalf("expected FailedTasks=20, got %d", gotRun.FailedTasks)
	}
	if len(gotScores) != total {
		t.Fatalf("expected %d scores, got %d", total, len(gotScores))
	}
}

func TestSQLiteStore_ReportAndList(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	run := &EvalRun{
		ID:         "run-rpt-1",
		Name:       "report test",
		Mode:       "fork",
		Status:     RunRunning,
		ConfigJSON: `{}`,
		TotalTasks: 1,
	}
	if err := st.CreateRun(ctx, run, nil); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	if err := st.UpdateRunStatus(ctx, run.ID, RunCompleted); err != nil {
		t.Fatalf("UpdateRunStatus: %v", err)
	}

	if err := st.SaveReport(ctx, &EvalReport{
		RunID:       run.ID,
		SummaryJSON: `{"accuracy":0.9}`,
		HTMLContent: "<html>v1</html>",
	}); err != nil {
		t.Fatalf("SaveReport 1: %v", err)
	}
	if err := st.SaveReport(ctx, &EvalReport{
		RunID:       run.ID,
		SummaryJSON: `{"accuracy":1.0}`,
		HTMLContent: "<html>v2</html>",
	}); err != nil {
		t.Fatalf("SaveReport 2: %v", err)
	}

	rpt, err := st.GetReport(ctx, run.ID)
	if err != nil {
		t.Fatalf("GetReport: %v", err)
	}
	if rpt.HTMLContent != "<html>v2</html>" {
		t.Fatalf("expected updated html, got %q", rpt.HTMLContent)
	}

	runs, err := st.ListRuns(ctx, 10)
	if err != nil || len(runs) != 1 || runs[0].Status != RunCompleted || runs[0].FinishedAt == nil {
		t.Fatalf("unexpected ListRuns result: runs=%+v err=%v", runs, err)
	}

	_, err = st.GetReport(ctx, "missing-id")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}
