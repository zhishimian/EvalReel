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

func TestBuildCallTreeAndHTMLWaterfall(t *testing.T) {
	// 正常 3 层树（planner -> coder -> reviewer）与时间偏移排序。
	spans := []store.TraceSpan{
		{SpanID: "sp-r", ParentSpanID: "sp-c", TraceID: "tr-1", AgentName: "reviewer", Model: "m1", StartTimeMs: 1300, DurationMs: 200, Status: "error", InputTokens: 30, OutputTokens: 10},
		{SpanID: "sp-p", ParentSpanID: "root-ext", TraceID: "tr-1", AgentName: "planner", Model: "m1", StartTimeMs: 1000, DurationMs: 500, Status: "ok", InputTokens: 10, OutputTokens: 20},
		{SpanID: "sp-c", ParentSpanID: "sp-p", TraceID: "tr-1", AgentName: "coder", Model: "m1", StartTimeMs: 1100, DurationMs: 150, Status: "ok", InputTokens: 20, OutputTokens: 40},
	}

	tree := BuildCallTree(spans)
	if len(tree) != 3 {
		t.Fatalf("expected 3 nodes, got %d", len(tree))
	}
	if tree[0].AgentName != "planner" || tree[0].Depth != 0 || tree[0].OffsetMs != 0 {
		t.Fatalf("unexpected root node: %+v", tree[0])
	}
	if tree[1].AgentName != "coder" || tree[1].Depth != 1 || tree[1].OffsetMs != 100 {
		t.Fatalf("unexpected coder node: %+v", tree[1])
	}
	if tree[2].AgentName != "reviewer" || tree[2].Depth != 2 || tree[2].Status != "error" {
		t.Fatalf("unexpected reviewer node: %+v", tree[2])
	}

	// 验证存在循环引用时 BuildCallTree 不会死循环。
	cyclic := []store.TraceSpan{
		{SpanID: "a", ParentSpanID: "b", AgentName: "agent-a", StartTimeMs: 10, DurationMs: 10, Status: "ok"},
		{SpanID: "b", ParentSpanID: "a", AgentName: "agent-b", StartTimeMs: 20, DurationMs: 10, Status: "ok"},
	}
	cTree := BuildCallTree(cyclic)
	if len(cTree) != 2 {
		t.Fatalf("expected cyclic graph to terminate with 2 nodes, got %d", len(cTree))
	}

	run := &store.EvalRun{ID: "run-tree-1", Name: "multi-agent-waterfall", Mode: "agent"}
	tasks := []store.EvalTask{
		{ID: "tk-1", RunID: run.ID, ItemID: "item1", TargetName: "multi-agent", TrialIndex: 0, Status: store.TaskSuccess, TraceID: "tr-1", TotalMs: 500, RequestSnapshot: `{"prompt":"build"}`},
	}
	sum := AggregateWithSpans(run, tasks, nil, map[string][]store.TraceSpan{"tr-1": spans})
	htmlStr, err := RenderHTML(sum)
	if err != nil {
		t.Fatalf("RenderHTML: %v", err)
	}
	for _, want := range []string{"调用层级树", "planner", "coder", "reviewer", "wf-bar-err", "padding-left:28px"} {
		if !strings.Contains(htmlStr, want) {
			t.Fatalf("waterfall html missing %q", want)
		}
	}
	if strings.Contains(htmlStr, "ZgotmplZ") {
		t.Fatalf("html/template produced unsafe ZgotmplZ escaping marker")
	}
}
