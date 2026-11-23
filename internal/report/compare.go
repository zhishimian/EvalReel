package report

import (
	"bytes"
	"fmt"
	"html/template"
	"os"
	"path/filepath"
	"sort"
	"time"

	"evalreel/internal/store"
)

// RunBundle 将一次历史评测的总任务、子任务与评分明细打包供跨版本对比使用。
type RunBundle struct {
	Run    *store.EvalRun
	Tasks  []store.EvalTask
	Scores []store.EvalScore
}

// VersionMetrics 保存单个历史版本在回归对比中的核心质量、延迟、花费与相邻增量指标。
type VersionMetrics struct {
	Index         int       `json:"index"`
	RunID         string    `json:"run_id"`
	Name          string    `json:"name"`
	TargetName    string    `json:"target_name"`
	CreatedAt     time.Time `json:"created_at"`
	TotalItems    int       `json:"total_items"`
	PassedItems   int       `json:"passed_items"`
	Accuracy      float64   `json:"accuracy"`
	PassAtK       float64   `json:"pass_at_k"`
	PassPowerK    float64   `json:"pass_power_k"`
	AvgTTFTMs     int64     `json:"avg_ttft_ms"`
	P50Ms         int64     `json:"p50_ms"`
	P95Ms         int64     `json:"p95_ms"`
	TotalCostUSD  float64   `json:"total_cost_usd"`
	HasDelta      bool      `json:"has_delta"`
	AccuracyDelta float64   `json:"accuracy_delta"`
	P95DeltaMs    int64     `json:"p95_delta_ms"`
	CostDeltaUSD  float64   `json:"cost_delta_usd"`
}

// CompareItemCell 表示单道题在某一个版本下的通过情况与评判摘要。
type CompareItemCell struct {
	RunID      string `json:"run_id"`
	Present    bool   `json:"present"`
	PassedAll  bool   `json:"passed_all"`
	PassCount  int    `json:"pass_count"`
	TrialCount int    `json:"trial_count"`
	AvgTotalMs int64  `json:"avg_total_ms"`
	Reason     string `json:"reason,omitempty"`
}

// CompareItemRow 表示单道题跨多个版本的并排回归对比行。
type CompareItemRow struct {
	ItemID        string            `json:"item_id"`
	PromptPreview string            `json:"prompt_preview"`
	ChangeType    string            `json:"change_type"` // regression | improved | stable_fail | stable_pass
	Cells         []CompareItemCell `json:"cells"`
}

// ComparisonSummary 汇总跨多个评测版本的总体走势与逐题退化/提升分类统计。
type ComparisonSummary struct {
	TargetFilter string           `json:"target_filter,omitempty"`
	GeneratedAt  time.Time        `json:"generated_at"`
	Versions     []VersionMetrics `json:"versions"`
	Regressions  int              `json:"regressions"`
	Improved     int              `json:"improved"`
	StableFail   int              `json:"stable_fail"`
	StablePass   int              `json:"stable_pass"`
	Items        []CompareItemRow `json:"items"`
}

// CompareRuns 对比 2 个及以上评测版本，算出版本间指标增量并将“回归退化题”排在最前。
func CompareRuns(bundles []RunBundle, targetFilter string) ComparisonSummary {
	comp := ComparisonSummary{
		TargetFilter: targetFilter,
		GeneratedAt:  time.Now().UTC(),
	}
	if len(bundles) == 0 {
		return comp
	}

	summaries := make([]Summary, len(bundles))
	itemPrompts := make(map[string]string)
	itemOrderMap := make(map[string]struct{})
	itemLookup := make([]map[string]ItemTargetResult, len(bundles))

	for i, b := range bundles {
		sum := Aggregate(b.Run, b.Tasks, b.Scores)
		summaries[i] = sum

		tgtSummary, chosenName := pickTargetSummary(sum.Targets, targetFilter)
		passedItems := 0
		itemMap := make(map[string]ItemTargetResult, len(sum.Items))
		for _, it := range sum.Items {
			itemOrderMap[it.ItemID] = struct{}{}
			if itemPrompts[it.ItemID] == "" && it.PromptPreview != "" {
				itemPrompts[it.ItemID] = it.PromptPreview
			}
			if chosenName != "" {
				if res, ok := it.Results[chosenName]; ok {
					itemMap[it.ItemID] = res
					if res.PassedAll {
						passedItems++
					}
				}
			}
		}
		itemLookup[i] = itemMap

		vm := VersionMetrics{
			Index:        i + 1,
			RunID:        b.Run.ID,
			Name:         b.Run.Name,
			TargetName:   chosenName,
			CreatedAt:    b.Run.CreatedAt,
			TotalItems:   sum.TotalItems,
			PassedItems:  passedItems,
			Accuracy:     tgtSummary.Accuracy,
			PassAtK:      tgtSummary.PassAtK,
			PassPowerK:   tgtSummary.PassPowerK,
			AvgTTFTMs:    tgtSummary.AvgTTFTMs,
			P50Ms:        tgtSummary.P50Ms,
			P95Ms:        tgtSummary.P95Ms,
			TotalCostUSD: tgtSummary.TotalCostUSD,
		}
		if i > 0 {
			prev := comp.Versions[i-1]
			vm.HasDelta = true
			vm.AccuracyDelta = vm.Accuracy - prev.Accuracy
			vm.P95DeltaMs = vm.P95Ms - prev.P95Ms
			vm.CostDeltaUSD = vm.TotalCostUSD - prev.TotalCostUSD
		}
		comp.Versions = append(comp.Versions, vm)
	}

	allItems := make([]string, 0, len(itemOrderMap))
	for id := range itemOrderMap {
		allItems = append(allItems, id)
	}
	sort.Strings(allItems)

	rows := make([]CompareItemRow, 0, len(allItems))
	for _, itemID := range allItems {
		cells := make([]CompareItemCell, len(bundles))
		for i, b := range bundles {
			if res, ok := itemLookup[i][itemID]; ok {
				cells[i] = CompareItemCell{
					RunID:      b.Run.ID,
					Present:    true,
					PassedAll:  res.PassedAll,
					PassCount:  res.PassCount,
					TrialCount: res.TrialCount,
					AvgTotalMs: res.AvgTotalMs,
					Reason:     res.SampleReason,
				}
			} else {
				cells[i] = CompareItemCell{RunID: b.Run.ID}
			}
		}

		changeType := classifyChange(cells)
		switch changeType {
		case "regression":
			comp.Regressions++
		case "improved":
			comp.Improved++
		case "stable_fail":
			comp.StableFail++
		case "stable_pass":
			comp.StablePass++
		}

		rows = append(rows, CompareItemRow{
			ItemID:        itemID,
			PromptPreview: itemPrompts[itemID],
			ChangeType:    changeType,
			Cells:         cells,
		})
	}

	sort.Slice(rows, func(i, j int) bool {
		pi, pj := changePriority(rows[i].ChangeType), changePriority(rows[j].ChangeType)
		if pi != pj {
			return pi < pj
		}
		return rows[i].ItemID < rows[j].ItemID
	})
	comp.Items = rows
	return comp
}

func pickTargetSummary(targets []TargetSummary, filter string) (TargetSummary, string) {
	if len(targets) == 0 {
		return TargetSummary{}, filter
	}
	if filter != "" {
		for _, t := range targets {
			if t.Name == filter {
				return t, t.Name
			}
		}
		return TargetSummary{Name: filter}, filter
	}
	return targets[0], targets[0].Name
}

func classifyChange(cells []CompareItemCell) string {
	if len(cells) < 2 {
		if len(cells) == 1 && cells[0].PassedAll {
			return "stable_pass"
		}
		return "stable_fail"
	}
	prev := cells[len(cells)-2]
	curr := cells[len(cells)-1]
	switch {
	case prev.PassedAll && !curr.PassedAll:
		return "regression"
	case !prev.PassedAll && curr.PassedAll:
		return "improved"
	case !prev.PassedAll && !curr.PassedAll:
		return "stable_fail"
	default:
		return "stable_pass"
	}
}

func changePriority(ct string) int {
	switch ct {
	case "regression":
		return 0
	case "improved":
		return 1
	case "stable_fail":
		return 2
	default:
		return 3
	}
}

type svgPoint struct {
	X          int
	AccY       int
	PowY       int
	Label      string
	AccText    string
	PowText    string
	HasNext    bool
	NextX      int
	NextAccY   int
	NextPowY   int
}

type compareHTMLView struct {
	ComparisonSummary
	GeneratedTime string
	SVGWidth      int
	Points        []svgPoint
}

const compareHTMLTemplate = `<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>EvalReel 多版本回归对比报告</title>
<style>
  body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; margin: 24px auto; max-width: 1200px; padding: 0 16px; color: #1f2937; background: #f9fafb; }
  .meta { color: #4b5563; font-size: 14px; margin-bottom: 20px; }
  .stats { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin-bottom: 20px; }
  .stat { background: #fff; border: 1px solid #e5e7eb; border-radius: 8px; padding: 14px; }
  .stat-val { font-size: 24px; font-weight: 700; margin-top: 4px; }
  .card { background: #fff; border: 1px solid #e5e7eb; border-radius: 8px; padding: 16px; margin-bottom: 20px; }
  table { width: 100%; border-collapse: collapse; font-size: 14px; }
  th, td { border-bottom: 1px solid #e5e7eb; padding: 10px 8px; text-align: left; vertical-align: top; }
  th { background: #f3f4f6; font-weight: 600; }
  .badge { display: inline-block; padding: 2px 8px; border-radius: 999px; font-size: 12px; font-weight: 600; }
  .b-regression { background: #fee2e2; color: #991b1b; }
  .b-improved { background: #d1fae5; color: #065f46; }
  .b-stable_fail { background: #fef3c7; color: #92400e; }
  .b-stable_pass { background: #e5e7eb; color: #374151; }
  .delta-pos { color: #047857; font-weight: 600; }
  .delta-neg { color: #b91c1c; font-weight: 600; }
  .toolbar { display: flex; gap: 8px; margin-bottom: 12px; }
  button { padding: 6px 12px; border: 1px solid #d1d5db; border-radius: 6px; background: #fff; cursor: pointer; font-size: 13px; }
  button.active { background: #1d4ed8; color: #fff; border-color: #1d4ed8; }
  .muted { color: #6b7280; font-size: 12px; }
  .hidden { display: none; }
</style>
</head>
<body>
  <h1>EvalReel 多版本回归趋势对比报告</h1>
  <div class="meta">对比版本数: {{len .Versions}} | 目标过滤: {{if .TargetFilter}}{{.TargetFilter}}{{else}}默认主目标{{end}} | 生成时间: {{.GeneratedTime}}</div>

  <div class="stats">
    <div class="stat"><div class="muted">回归退化 (Regression)</div><div class="stat-val" style="color:#b91c1c;">{{.Regressions}}</div></div>
    <div class="stat"><div class="muted">修复提升 (Improved)</div><div class="stat-val" style="color:#047857;">{{.Improved}}</div></div>
    <div class="stat"><div class="muted">持续失败 (Stable Fail)</div><div class="stat-val" style="color:#92400e;">{{.StableFail}}</div></div>
    <div class="stat"><div class="muted">持续稳定 (Stable Pass)</div><div class="stat-val">{{.StablePass}}</div></div>
  </div>

  <div class="card">
    <h2>1. 版本指标与相邻增量走势</h2>
    <table>
      <thead>
        <tr>
          <th>版本顺序</th>
          <th>Run ID / 名称</th>
          <th>目标名</th>
          <th>准确率 (Accuracy)</th>
          <th>稳定性 (pass^k)</th>
          <th>P50 / P95 耗时</th>
          <th>预估花费</th>
        </tr>
      </thead>
      <tbody>
        {{range .Versions}}
        <tr>
          <td><strong>v{{.Index}}</strong></td>
          <td><code>{{.RunID}}</code><div class="muted">{{.Name}}</div></td>
          <td>{{.TargetName}}</td>
          <td>
            {{printf "%.1f%%" .Accuracy}}
            {{if .HasDelta}}<span class="{{if ge .AccuracyDelta 0.0}}delta-pos{{else}}delta-neg{{end}}">({{printf "%+.1f%%" .AccuracyDelta}})</span>{{end}}
          </td>
          <td>{{printf "%.1f%%" .PassPowerK}}</td>
          <td>
            {{.P50Ms}} / {{.P95Ms}} ms
            {{if .HasDelta}}<div class="muted">P95 {{printf "%+d" .P95DeltaMs}} ms</div>{{end}}
          </td>
          <td>
            {{printf "%.4f" .TotalCostUSD}}
            {{if .HasDelta}}<div class="muted">{{printf "%+.4f" .CostDeltaUSD}}</div>{{end}}
          </td>
        </tr>
        {{end}}
      </tbody>
    </table>
  </div>

  <div class="card">
    <h2>2. 版本准确率与稳定性折线图（离线内联 SVG）</h2>
    <svg width="100%" height="200" viewBox="0 0 {{.SVGWidth}} 200" xmlns="http://www.w3.org/2000/svg">
      <rect x="0" y="0" width="{{.SVGWidth}}" height="200" fill="#ffffff"/>
      <text x="20" y="20" font-size="12" fill="#2563eb">● 准确率 Accuracy</text>
      <text x="150" y="20" font-size="12" fill="#10b981">● 稳定性 pass^k</text>
      <line x1="40" y1="40" x2="40" y2="160" stroke="#d1d5db" stroke-width="1"/>
      <line x1="40" y1="160" x2="{{.SVGWidth}}" y2="160" stroke="#d1d5db" stroke-width="1"/>
      {{range .Points}}
        {{if .HasNext}}
        <line x1="{{.X}}" y1="{{.AccY}}" x2="{{.NextX}}" y2="{{.NextAccY}}" stroke="#2563eb" stroke-width="2"/>
        <line x1="{{.X}}" y1="{{.PowY}}" x2="{{.NextX}}" y2="{{.NextPowY}}" stroke="#10b981" stroke-width="2" stroke-dasharray="4 3"/>
        {{end}}
        <circle cx="{{.X}}" cy="{{.AccY}}" r="4" fill="#2563eb"/>
        <circle cx="{{.X}}" cy="{{.PowY}}" r="4" fill="#10b981"/>
        <text x="{{.X}}" y="180" font-size="12" text-anchor="middle" fill="#374151">{{.Label}}</text>
        <text x="{{.X}}" y="{{.AccY}}" dy="-8" font-size="11" text-anchor="middle" fill="#1d4ed8">{{.AccText}}</text>
      {{end}}
    </svg>
  </div>

  <div class="card">
    <h2>3. 逐题退化与提升明细（回归退化置顶）</h2>
    <div class="toolbar">
      <button id="c-all" class="active" onclick="filterComp('all')">全部题目 ({{len .Items}})</button>
      <button id="c-reg" onclick="filterComp('regression')">仅看回归退化 ({{.Regressions}})</button>
      <button id="c-chg" onclick="filterComp('changed')">仅看有变化题目</button>
    </div>
    <table>
      <thead>
        <tr>
          <th style="width:220px;">题目与状态变化</th>
          {{range .Versions}}
          <th>v{{.Index}} ({{.Name}})</th>
          {{end}}
        </tr>
      </thead>
      <tbody>
        {{range .Items}}
        <tr class="comp-row" data-change="{{.ChangeType}}">
          <td>
            <strong>{{.ItemID}}</strong>
            <span class="badge b-{{.ChangeType}}">{{.ChangeType}}</span>
            <div class="muted">{{.PromptPreview}}</div>
          </td>
          {{range .Cells}}
          <td>
            {{if .Present}}
              {{if .PassedAll}}<span class="badge b-improved">通过 {{.PassCount}}/{{.TrialCount}}</span>
              {{else}}<span class="badge b-regression">失败 {{.PassCount}}/{{.TrialCount}}</span>{{end}}
              <span class="muted">({{.AvgTotalMs}} ms)</span>
              {{if .Reason}}<div class="muted">{{.Reason}}</div>{{end}}
            {{else}}
              <span class="muted">无记录</span>
            {{end}}
          </td>
          {{end}}
        </tr>
        {{end}}
      </tbody>
    </table>
  </div>

  <script>
    function filterComp(mode) {
      document.getElementById('c-all').classList.toggle('active', mode === 'all');
      document.getElementById('c-reg').classList.toggle('active', mode === 'regression');
      document.getElementById('c-chg').classList.toggle('active', mode === 'changed');
      document.querySelectorAll('.comp-row').forEach(function(row) {
        var chg = row.getAttribute('data-change');
        var show = mode === 'all' || (mode === 'regression' && chg === 'regression') || (mode === 'changed' && (chg === 'regression' || chg === 'improved'));
        row.classList.toggle('hidden', !show);
      });
    }
  </script>
</body>
</html>`

// RenderCompareHTML 生成零外部依赖的单文件离线回归对比 HTML 页面。
func RenderCompareHTML(comp ComparisonSummary) (string, error) {
	tmpl, err := template.New("compare").Parse(compareHTMLTemplate)
	if err != nil {
		return "", fmt.Errorf("parse compare html template: %w", err)
	}

	n := len(comp.Versions)
	width := 760
	if n > 4 {
		width = 160 + n*140
	}
	points := make([]svgPoint, n)
	spanX := width - 120
	for i, vm := range comp.Versions {
		x := 80
		if n > 1 {
			x = 80 + i*spanX/(n-1)
		}
		accY := 160 - int(vm.Accuracy*1.1)
		powY := 160 - int(vm.PassPowerK*1.1)
		points[i] = svgPoint{
			X:       x,
			AccY:    accY,
			PowY:    powY,
			Label:   fmt.Sprintf("v%d", vm.Index),
			AccText: fmt.Sprintf("%.1f%%", vm.Accuracy),
			PowText: fmt.Sprintf("%.1f%%", vm.PassPowerK),
		}
	}
	for i := 0; i+1 < n; i++ {
		points[i].HasNext = true
		points[i].NextX = points[i+1].X
		points[i].NextAccY = points[i+1].AccY
		points[i].NextPowY = points[i+1].PowY
	}

	view := compareHTMLView{
		ComparisonSummary: comp,
		GeneratedTime:     comp.GeneratedAt.Format("2006-01-02 15:04:05 UTC"),
		SVGWidth:          width,
		Points:            points,
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, view); err != nil {
		return "", fmt.Errorf("execute compare html template: %w", err)
	}
	return buf.String(), nil
}

// WriteCompareHTMLFile 将多版本对比 HTML 战报写入本地磁盘指定路径。
func WriteCompareHTMLFile(outPath string, htmlContent string) (string, error) {
	if outPath == "" {
		outPath = filepath.Join("./reports", fmt.Sprintf("compare_%s.html", time.Now().Format("20060102_150405")))
	}
	dir := filepath.Dir(outPath)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return "", fmt.Errorf("create compare report dir %s: %w", dir, err)
		}
	}
	if err := os.WriteFile(outPath, []byte(htmlContent), 0o644); err != nil {
		return "", fmt.Errorf("write compare report %s: %w", outPath, err)
	}
	return outPath, nil
}
