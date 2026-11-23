package report

import (
	"sort"

	"evalreel/internal/store"
)

// SpanNode 表示组装好的多 Agent 调用树中的单个节点（含缩进深度和瀑布图时间轴占比）。
type SpanNode struct {
	SpanID       string  `json:"span_id"`
	ParentSpanID string  `json:"parent_span_id,omitempty"`
	AgentName    string  `json:"agent_name"`
	Model        string  `json:"model,omitempty"`
	Status       string  `json:"status"`
	Depth        int     `json:"depth"`
	OffsetMs     int64   `json:"offset_ms"`
	DurationMs   int64   `json:"duration_ms"`
	TTFTMs       int64   `json:"ttft_ms"`
	InputTokens  int     `json:"input_tokens"`
	OutputTokens int     `json:"output_tokens"`
	LeftPct      float64 `json:"left_pct"`
	WidthPct     float64 `json:"width_pct"`
	ErrorMsg     string  `json:"error_msg,omitempty"`
}

// BuildCallTree 将同一条链路下的 TraceSpan 列表组装成带深度与时间偏移占比的扁平树序。
// 若存在缺失父节点的孤儿节点或异常循环引用，会自动降级挂载并截断防死循环。
func BuildCallTree(spans []store.TraceSpan) []SpanNode {
	if len(spans) == 0 {
		return nil
	}

	sorted := append([]store.TraceSpan(nil), spans...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].StartTimeMs != sorted[j].StartTimeMs {
			return sorted[i].StartTimeMs < sorted[j].StartTimeMs
		}
		return sorted[i].SpanID < sorted[j].SpanID
	})

	byID := make(map[string]store.TraceSpan, len(sorted))
	minStart := sorted[0].StartTimeMs
	var maxEnd int64

	for _, sp := range sorted {
		byID[sp.SpanID] = sp
		if sp.StartTimeMs < minStart {
			minStart = sp.StartTimeMs
		}
		dur := sp.DurationMs
		if dur < 0 {
			dur = 0
		}
		if end := sp.StartTimeMs + dur; end > maxEnd {
			maxEnd = end
		}
	}

	totalWindow := maxEnd - minStart
	if totalWindow <= 0 {
		totalWindow = 1
	}

	children := make(map[string][]store.TraceSpan)
	var roots []store.TraceSpan
	for _, sp := range sorted {
		pid := sp.ParentSpanID
		if pid != "" && pid != sp.SpanID {
			if _, ok := byID[pid]; ok {
				children[pid] = append(children[pid], sp)
				continue
			}
		}
		roots = append(roots, sp)
	}

	visited := make(map[string]bool, len(sorted))
	out := make([]SpanNode, 0, len(sorted))

	var walk func(sp store.TraceSpan, depth int)
	walk = func(sp store.TraceSpan, depth int) {
		if visited[sp.SpanID] {
			return
		}
		visited[sp.SpanID] = true

		offsetMs := sp.StartTimeMs - minStart
		if offsetMs < 0 {
			offsetMs = 0
		}
		dur := sp.DurationMs
		if dur < 0 {
			dur = 0
		}

		leftPct := 100.0 * float64(offsetMs) / float64(totalWindow)
		if leftPct < 0 {
			leftPct = 0
		}
		if leftPct > 98.0 {
			leftPct = 98.0
		}

		widthPct := 100.0 * float64(dur) / float64(totalWindow)
		if widthPct < 2.0 {
			widthPct = 2.0
		}
		if leftPct+widthPct > 100.0 {
			widthPct = 100.0 - leftPct
		}

		agent := sp.AgentName
		if agent == "" {
			agent = sp.Operation
		}
		if agent == "" {
			agent = "llm"
		}

		out = append(out, SpanNode{
			SpanID:       sp.SpanID,
			ParentSpanID: sp.ParentSpanID,
			AgentName:    agent,
			Model:        sp.Model,
			Status:       sp.Status,
			Depth:        depth,
			OffsetMs:     offsetMs,
			DurationMs:   dur,
			TTFTMs:       sp.TTFTMs,
			InputTokens:  sp.InputTokens,
			OutputTokens: sp.OutputTokens,
			LeftPct:      leftPct,
			WidthPct:     widthPct,
			ErrorMsg:     sp.ErrorMsg,
		})

		for _, ch := range children[sp.SpanID] {
			walk(ch, depth+1)
		}
	}

	for _, rt := range roots {
		walk(rt, 0)
	}
	// 处理存在纯环路（没有根触发到）的残留节点，确保调用节点不丢失。
	for _, sp := range sorted {
		if !visited[sp.SpanID] {
			walk(sp, 0)
		}
	}
	return out
}
