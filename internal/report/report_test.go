package report

import (
	"math"
	"strings"
	"testing"

	"evalreel/internal/store"
)

func TestAggregate_MetricsAndPassK(t *testing.T) {
	run := &store.EvalRun{
		ID:   "run-agg-1",
		Name: "agg test",
		Mode: "benchmark",
	}

	// 2 个模型 × 2 道题 × 每题 3 遍 = 12 条子任务
	// qwen3.8-flash: item1 3/3 通过，item2 1/3 通过 -> Accuracy=4/6(66.7%), pass@3=100%, pass^3=50%
	// ds-flash:      item1 3/3 通过，item2 0/3 通过 -> Accuracy=3/6(50.0%), pass@3=50%,  pass^3=50%
	tasks := []store.EvalTask{
		{ID: "q-1-0", RunID: run.ID, ItemID: "item1", TargetName: "qwen3.8-flash", TrialIndex: 0, Status: store.TaskSuccess, TTFTMs: 200, TotalMs: 500, InputTokens: 10, OutputTokens: 20, CostUSD: 0.001, RequestSnapshot: `{"prompt":"Q1"}`},
		{ID: "q-1-1", RunID: run.ID, ItemID: "item1", TargetName: "qwen3.8-flash", TrialIndex: 1, Status: store.TaskSuccess, TTFTMs: 300, TotalMs: 600, InputTokens: 10, OutputTokens: 20, CostUSD: 0.001, RequestSnapshot: `{"prompt":"Q1"}`},
		{ID: "q-1-2", RunID: run.ID, ItemID: "item1", TargetName: "qwen3.8-flash", TrialIndex: 2, Status: store.TaskSuccess, TTFTMs: 400, TotalMs: 700, InputTokens: 10, OutputTokens: 20, CostUSD: 0.001, RequestSnapshot: `{"prompt":"Q1"}`},
		{ID: "q-2-0", RunID: run.ID, ItemID: "item2", TargetName: "qwen3.8-flash", TrialIndex: 0, Status: store.TaskSuccess, TTFTMs: 200, TotalMs: 800, InputTokens: 10, OutputTokens: 20, CostUSD: 0.001, RequestSnapshot: `{"prompt":"Q2"}`},
		{ID: "q-2-1", RunID: run.ID, ItemID: "item2", TargetName: "qwen3.8-flash", TrialIndex: 1, Status: store.TaskSuccess, TTFTMs: 200, TotalMs: 900, InputTokens: 10, OutputTokens: 20, CostUSD: 0.001, RequestSnapshot: `{"prompt":"Q2"}`},
		{ID: "q-2-2", RunID: run.ID, ItemID: "item2", TargetName: "qwen3.8-flash", TrialIndex: 2, Status: store.TaskSuccess, TTFTMs: 200, TotalMs: 1000, InputTokens: 10, OutputTokens: 20, CostUSD: 0.001, RequestSnapshot: `{"prompt":"Q2"}`},

		{ID: "d-1-0", RunID: run.ID, ItemID: "item1", TargetName: "deepseek-v4.1-flash-1017", TrialIndex: 0, Status: store.TaskSuccess, TTFTMs: 500, TotalMs: 1100, InputTokens: 10, OutputTokens: 20, CostUSD: 0.002, RequestSnapshot: `{"prompt":"Q1"}`},
		{ID: "d-1-1", RunID: run.ID, ItemID: "item1", TargetName: "deepseek-v4.1-flash-1017", TrialIndex: 1, Status: store.TaskSuccess, TTFTMs: 500, TotalMs: 1200, InputTokens: 10, OutputTokens: 20, CostUSD: 0.002, RequestSnapshot: `{"prompt":"Q1"}`},
		{ID: "d-1-2", RunID: run.ID, ItemID: "item1", TargetName: "deepseek-v4.1-flash-1017", TrialIndex: 2, Status: store.TaskSuccess, TTFTMs: 500, TotalMs: 1300, InputTokens: 10, OutputTokens: 20, CostUSD: 0.002, RequestSnapshot: `{"prompt":"Q1"}`},
		{ID: "d-2-0", RunID: run.ID, ItemID: "item2", TargetName: "deepseek-v4.1-flash-1017", TrialIndex: 0, Status: store.TaskFailed, ErrorMsg: "timeout", TotalMs: 1400, RequestSnapshot: `{"prompt":"Q2"}`},
		{ID: "d-2-1", RunID: run.ID, ItemID: "item2", TargetName: "deepseek-v4.1-flash-1017", TrialIndex: 1, Status: store.TaskFailed, ErrorMsg: "timeout", TotalMs: 1500, RequestSnapshot: `{"prompt":"Q2"}`},
		{ID: "d-2-2", RunID: run.ID, ItemID: "item2", TargetName: "deepseek-v4.1-flash-1017", TrialIndex: 2, Status: store.TaskFailed, ErrorMsg: "timeout", TotalMs: 1600, RequestSnapshot: `{"prompt":"Q2"}`},
	}

	scores := []store.EvalScore{
		{TaskID: "q-1-0", RunID: run.ID, ScorerName: "rule", Passed: true},
		{TaskID: "q-1-1", RunID: run.ID, ScorerName: "rule", Passed: true},
		{TaskID: "q-1-2", RunID: run.ID, ScorerName: "rule", Passed: true},
		{TaskID: "q-2-0", RunID: run.ID, ScorerName: "rule", Passed: true},
		{TaskID: "q-2-1", RunID: run.ID, ScorerName: "rule", Passed: false, Reason: "wrong arg"},
		{TaskID: "q-2-2", RunID: run.ID, ScorerName: "rule", Passed: false, Reason: "wrong arg"},
		{TaskID: "d-1-0", RunID: run.ID, ScorerName: "rule", Passed: true},
		{TaskID: "d-1-1", RunID: run.ID, ScorerName: "rule", Passed: true},
		{TaskID: "d-1-2", RunID: run.ID, ScorerName: "rule", Passed: true},
	}

	sum := Aggregate(run, tasks, scores)
	if sum.Trials != 3 || sum.TotalItems != 2 || len(sum.Targets) != 2 {
		t.Fatalf("unexpected summary header: %+v", sum)
	}

	q := sum.Targets[0]
	if math.Abs(q.Accuracy-66.666) > 0.2 || q.PassAtK != 100 || q.PassPowerK != 50 {
		t.Fatalf("unexpected qwen metrics: %+v", q)
	}
	if q.P50Ms != 700 || q.P95Ms != 1000 {
		t.Fatalf("unexpected qwen percentiles: P50=%d P95=%d", q.P50Ms, q.P95Ms)
	}

	d := sum.Targets[1]
	if d.Accuracy != 50 || d.PassAtK != 50 || d.PassPowerK != 50 {
		t.Fatalf("unexpected deepseek metrics: %+v", d)
	}

	if sum.Items[0].HasFailure || sum.Items[0].HasDisagreement {
		t.Fatalf("item1 should have no failure or disagreement")
	}
	if !sum.Items[1].HasFailure {
		t.Fatalf("item2 should mark HasFailure=true")
	}

	htmlStr, err := RenderHTML(sum)
	if err != nil {
		t.Fatalf("RenderHTML: %v", err)
	}
	for _, want := range []string{"<svg", "qwen3.8-flash", "deepseek-v4.1-flash-1017", "filterRows"} {
		if !strings.Contains(htmlStr, want) {
			t.Fatalf("rendered html missing %q", want)
		}
	}
	if strings.Contains(htmlStr, "<script src=") || strings.Contains(htmlStr, "<link ") || strings.Contains(htmlStr, "cdn.") {
		t.Fatalf("offline report html must not contain external script/css CDN dependencies")
	}
}
