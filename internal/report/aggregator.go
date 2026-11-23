package report

import (
	"encoding/json"
	"math"
	"sort"
	"strings"
	"time"

	"evalreel/internal/model"
	"evalreel/internal/store"
)

// Summary 保存一场评测汇总后的全局指标、各模型对比表以及逐题差异明细。
type Summary struct {
	RunID       string           `json:"run_id"`
	Name        string           `json:"name"`
	Mode        string           `json:"mode"`
	Status      store.RunStatus  `json:"status"`
	Trials      int              `json:"trials"`
	TotalItems  int              `json:"total_items"`
	GeneratedAt time.Time        `json:"generated_at"`
	Targets     []TargetSummary  `json:"targets"`
	Items       []ItemComparison `json:"items"`
}

// TargetSummary 汇总单个被测模型或 Agent 在整场评测中的表现与成本。
type TargetSummary struct {
	Name              string  `json:"name"`
	TotalTasks        int     `json:"total_tasks"`
	SuccessTasks      int     `json:"success_tasks"`
	PassedTasks       int     `json:"passed_tasks"`
	Accuracy          float64 `json:"accuracy"`       // 百分比 0~100
	PassAtK           float64 `json:"pass_at_k"`      // k 次中至少对 1 次的题目占比（百分比）
	PassPowerK        float64 `json:"pass_power_k"`   // k 次全部做对的题目占比（百分比）
	AvgTTFTMs         int64   `json:"avg_ttft_ms"`
	P50Ms             int64   `json:"p50_ms"`
	P95Ms             int64   `json:"p95_ms"`
	TotalInputTokens  int     `json:"total_input_tokens"`
	TotalOutputTokens int     `json:"total_output_tokens"`
	TotalCostUSD      float64 `json:"total_cost_usd"`
}

// ItemComparison 展示同一道题目在不同目标之间的并排对比结果。
type ItemComparison struct {
	ItemID          string                      `json:"item_id"`
	PromptPreview   string                      `json:"prompt_preview"`
	HasDisagreement bool                        `json:"has_disagreement"`
	HasFailure      bool                        `json:"has_failure"`
	Results         map[string]ItemTargetResult `json:"results"`
	OrderedResults  []ItemTargetResult          `json:"ordered_results"`
}

// ItemTargetResult 保存某个目标在单道题（含多次重复）上的汇总输出与评判理由。
type ItemTargetResult struct {
	TargetName   string     `json:"target_name"`
	PassCount    int        `json:"pass_count"`
	TrialCount   int        `json:"trial_count"`
	PassedAll    bool       `json:"passed_all"`
	PassedAny    bool       `json:"passed_any"`
	SampleOutput string     `json:"sample_output"`
	SampleTools  string     `json:"sample_tools"`
	SampleReason string     `json:"sample_reason"`
	AvgTotalMs   int64      `json:"avg_total_ms"`
	CallTree     []SpanNode `json:"call_tree,omitempty"`
}

// Aggregate 根据数据库落盘的子任务与评判得分，算出准确率、pass@k、pass^k、分位延迟与差异明细。
func Aggregate(run *store.EvalRun, tasks []store.EvalTask, scores []store.EvalScore) Summary {
	return AggregateWithSpans(run, tasks, scores, nil)
}

// AggregateWithSpans 在汇总基础指标的同时，根据 trace_id 关联的调用节点组装多 Agent 瀑布调用树。
func AggregateWithSpans(
	run *store.EvalRun,
	tasks []store.EvalTask,
	scores []store.EvalScore,
	spansByTrace map[string][]store.TraceSpan,
) Summary {
	scoresByTask := make(map[string][]store.EvalScore)
	for _, sc := range scores {
		scoresByTask[sc.TaskID] = append(scoresByTask[sc.TaskID], sc)
	}

	targetOrder := make([]string, 0)
	targetSeen := make(map[string]bool)
	itemOrder := make([]string, 0)
	itemSeen := make(map[string]bool)
	itemPrompts := make(map[string]string)

	// target -> item -> []task
	grouped := make(map[string]map[string][]store.EvalTask)
	maxTrial := 0

	for _, tk := range tasks {
		if tk.TrialIndex > maxTrial {
			maxTrial = tk.TrialIndex
		}
		if !targetSeen[tk.TargetName] {
			targetSeen[tk.TargetName] = true
			targetOrder = append(targetOrder, tk.TargetName)
			grouped[tk.TargetName] = make(map[string][]store.EvalTask)
		}
		if !itemSeen[tk.ItemID] {
			itemSeen[tk.ItemID] = true
			itemOrder = append(itemOrder, tk.ItemID)
			itemPrompts[tk.ItemID] = extractPromptPreview(tk.RequestSnapshot)
		}
		grouped[tk.TargetName][tk.ItemID] = append(grouped[tk.TargetName][tk.ItemID], tk)
	}

	sort.Strings(itemOrder)

	taskPassedMap := make(map[string]bool, len(tasks))
	taskReasonMap := make(map[string]string, len(tasks))
	for _, tk := range tasks {
		passed, reason := evaluateTaskPassed(tk, scoresByTask[tk.ID])
		taskPassedMap[tk.ID] = passed
		taskReasonMap[tk.ID] = reason
	}

	targetSummaries := make([]TargetSummary, 0, len(targetOrder))
	for _, tName := range targetOrder {
		ts := TargetSummary{Name: tName}
		var (
			ttftSum   int64
			ttftCount int64
			latencies []int64
			anyItems  int
			allItems  int
		)

		itemMap := grouped[tName]
		for _, itemID := range itemOrder {
			trials := itemMap[itemID]
			if len(trials) == 0 {
				continue
			}
			itemAny := false
			itemAll := true
			for _, tk := range trials {
				ts.TotalTasks++
				if tk.Status == store.TaskSuccess {
					ts.SuccessTasks++
				}
				if taskPassedMap[tk.ID] {
					ts.PassedTasks++
					itemAny = true
				} else {
					itemAll = false
				}
				if tk.TTFTMs > 0 {
					ttftSum += tk.TTFTMs
					ttftCount++
				}
				if tk.TotalMs > 0 {
					latencies = append(latencies, tk.TotalMs)
				}
				ts.TotalInputTokens += tk.InputTokens
				ts.TotalOutputTokens += tk.OutputTokens
				ts.TotalCostUSD += tk.CostUSD
			}
			if itemAny {
				anyItems++
			}
			if itemAll {
				allItems++
			}
		}

		if ts.TotalTasks > 0 {
			ts.Accuracy = 100.0 * float64(ts.PassedTasks) / float64(ts.TotalTasks)
		}
		distinctItems := len(itemMap)
		if distinctItems > 0 {
			ts.PassAtK = 100.0 * float64(anyItems) / float64(distinctItems)
			ts.PassPowerK = 100.0 * float64(allItems) / float64(distinctItems)
		}
		if ttftCount > 0 {
			ts.AvgTTFTMs = ttftSum / ttftCount
		}
		ts.P50Ms = percentile(latencies, 0.50)
		ts.P95Ms = percentile(latencies, 0.95)
		targetSummaries = append(targetSummaries, ts)
	}

	itemsOut := make([]ItemComparison, 0, len(itemOrder))
	for _, itemID := range itemOrder {
		comp := ItemComparison{
			ItemID:         itemID,
			PromptPreview:  itemPrompts[itemID],
			Results:        make(map[string]ItemTargetResult, len(targetOrder)),
			OrderedResults: make([]ItemTargetResult, 0, len(targetOrder)),
		}
		var hasPassTarget, hasFailTarget bool
		for _, tName := range targetOrder {
			trials := grouped[tName][itemID]
			res := ItemTargetResult{
				TargetName: tName,
				TrialCount: len(trials),
				PassedAll:  len(trials) > 0,
			}
			var latSum int64
			for idx, tk := range trials {
				passed := taskPassedMap[tk.ID]
				if passed {
					res.PassCount++
					res.PassedAny = true
				} else {
					res.PassedAll = false
				}
				latSum += tk.TotalMs
				if idx == 0 {
					res.SampleOutput = truncateStr(tk.ResponseText, 240)
					res.SampleTools = formatToolsCompact(tk.ToolCallsJSON)
					res.SampleReason = taskReasonMap[tk.ID]
					if tk.TraceID != "" && len(spansByTrace) > 0 {
						res.CallTree = BuildCallTree(spansByTrace[tk.TraceID])
					}
				}
			}
			if res.TrialCount > 0 {
				res.AvgTotalMs = latSum / int64(res.TrialCount)
			}
			if res.PassedAll {
				hasPassTarget = true
			} else {
				hasFailTarget = true
				comp.HasFailure = true
			}
			comp.Results[tName] = res
			comp.OrderedResults = append(comp.OrderedResults, res)
		}
		comp.HasDisagreement = hasPassTarget && hasFailTarget
		itemsOut = append(itemsOut, comp)
	}

	return Summary{
		RunID:       run.ID,
		Name:        run.Name,
		Mode:        run.Mode,
		Status:      run.Status,
		Trials:      maxTrial + 1,
		TotalItems:  len(itemOrder),
		GeneratedAt: time.Now().UTC(),
		Targets:     targetSummaries,
		Items:       itemsOut,
	}
}

func evaluateTaskPassed(tk store.EvalTask, scores []store.EvalScore) (bool, string) {
	if tk.Status != store.TaskSuccess {
		if tk.ErrorMsg != "" {
			return false, tk.ErrorMsg
		}
		return false, "task status: " + string(tk.Status)
	}
	if len(scores) == 0 {
		return true, "completed"
	}
	var reasons []string
	allPassed := true
	for _, sc := range scores {
		if !sc.Passed {
			allPassed = false
		}
		if sc.Reason != "" {
			reasons = append(reasons, sc.ScorerName+": "+sc.Reason)
		}
	}
	return allPassed, strings.Join(reasons, "; ")
}

func percentile(values []int64, p float64) int64 {
	if len(values) == 0 {
		return 0
	}
	cp := append([]int64(nil), values...)
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	idx := int(math.Ceil(p*float64(len(cp)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(cp) {
		idx = len(cp) - 1
	}
	return cp[idx]
}

func extractPromptPreview(snapshot string) string {
	if snapshot == "" {
		return ""
	}
	var item struct {
		Prompt   string `json:"prompt"`
		Messages []struct {
			Role    string `json:"role"`
			Content string `json:"content"`
		} `json:"messages"`
	}
	if err := json.Unmarshal([]byte(snapshot), &item); err != nil {
		return truncateStr(snapshot, 120)
	}
	if item.Prompt != "" {
		return truncateStr(item.Prompt, 120)
	}
	for i := len(item.Messages) - 1; i >= 0; i-- {
		if item.Messages[i].Role == "user" && item.Messages[i].Content != "" {
			return truncateStr(item.Messages[i].Content, 120)
		}
	}
	return ""
}

func formatToolsCompact(raw string) string {
	if raw == "" || raw == "null" || raw == "[]" {
		return ""
	}
	var calls []model.ToolCall
	if err := json.Unmarshal([]byte(raw), &calls); err != nil || len(calls) == 0 {
		return raw
	}
	parts := make([]string, 0, len(calls))
	for _, c := range calls {
		parts = append(parts, c.Name+"("+c.Arguments+")")
	}
	return strings.Join(parts, ", ")
}

func truncateStr(s string, maxRunes int) string {
	r := []rune(strings.TrimSpace(s))
	if len(r) <= maxRunes {
		return string(r)
	}
	return string(r[:maxRunes]) + "..."
}
