package report

import (
	"bytes"
	"encoding/json"
	"fmt"
	"html/template"
	"os"
	"path/filepath"
	"strings"
)

const reportHTMLTemplate = `<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>EvalReel Report - {{.Name}}</title>
<style>
  body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; margin: 24px auto; max-width: 1200px; padding: 0 16px; color: #1f2937; background: #f9fafb; }
  h1, h2 { margin-bottom: 8px; }
  .meta { color: #4b5563; font-size: 14px; margin-bottom: 24px; }
  .card { background: #ffffff; border: 1px solid #e5e7eb; border-radius: 8px; padding: 16px; margin-bottom: 20px; }
  table { width: 100%; border-collapse: collapse; font-size: 14px; }
  th, td { border-bottom: 1px solid #e5e7eb; padding: 10px 8px; text-align: left; vertical-align: top; }
  th { background: #f3f4f6; font-weight: 600; }
  .badge-pass { display: inline-block; padding: 2px 8px; border-radius: 999px; font-size: 12px; background: #d1fae5; color: #065f46; font-weight: 600; }
  .badge-fail { display: inline-block; padding: 2px 8px; border-radius: 999px; font-size: 12px; background: #fee2e2; color: #991b1b; font-weight: 600; }
  .badge-warn { display: inline-block; padding: 2px 8px; border-radius: 999px; font-size: 12px; background: #fef3c7; color: #92400e; font-weight: 600; }
  .toolbar { display: flex; gap: 8px; margin-bottom: 12px; }
  button { padding: 6px 12px; border: 1px solid #d1d5db; border-radius: 6px; background: #fff; cursor: pointer; font-size: 13px; }
  button.active { background: #1d4ed8; color: #fff; border-color: #1d4ed8; }
  .mono { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 12px; white-space: pre-wrap; word-break: break-word; color: #374151; }
  .muted { color: #6b7280; font-size: 12px; }
  .hidden { display: none; }
</style>
</head>
<body>
  <h1>EvalReel 评测报告：{{.Name}}</h1>
  <div class="meta">
    Run ID: <code>{{.RunID}}</code> | 模式: <strong>{{.Mode}}</strong> | 题目数: {{.TotalItems}} | 每题次数(k): {{.Trials}} | 生成时间: {{.GeneratedTime}}
  </div>

  <div class="card">
    <h2>1. 模型横向汇总</h2>
    <table>
      <thead>
        <tr>
          <th>目标名称</th>
          <th>准确率 (Accuracy)</th>
          <th>pass@{{.Trials}}</th>
          <th>pass^{{.Trials}} (稳定性)</th>
          <th>平均首字 (TTFT)</th>
          <th>P50 / P95 耗时</th>
          <th>输入 / 输出词元</th>
          <th>预估花费</th>
        </tr>
      </thead>
      <tbody>
        {{range .Targets}}
        <tr>
          <td><strong>{{.Name}}</strong><div class="muted">{{.PassedTasks}}/{{.TotalTasks}} 通过</div></td>
          <td>{{printf "%.1f%%" .Accuracy}}</td>
          <td>{{printf "%.1f%%" .PassAtK}}</td>
          <td>{{printf "%.1f%%" .PassPowerK}}</td>
          <td>{{.AvgTTFTMs}} ms</td>
          <td>{{.P50Ms}} / {{.P95Ms}} ms</td>
          <td>{{.TotalInputTokens}} / {{.TotalOutputTokens}}</td>
          <td>{{printf "%.4f" .TotalCostUSD}}</td>
        </tr>
        {{end}}
      </tbody>
    </table>
  </div>

  <div class="card">
    <h2>2. 通过率与稳定性对比图（离线内联 SVG）</h2>
    <svg width="100%" height="{{.SVGHeight}}" viewBox="0 0 760 {{.SVGHeight}}" xmlns="http://www.w3.org/2000/svg">
      <rect x="0" y="0" width="760" height="{{.SVGHeight}}" fill="#ffffff"/>
      <text x="20" y="20" font-size="12" fill="#2563eb">■ 准确率 Accuracy</text>
      <text x="150" y="20" font-size="12" fill="#10b981">■ 稳定性 pass^{{.Trials}}</text>
      {{range .Bars}}
      <text x="20" y="{{.LabelY}}" font-size="13" fill="#111827" font-weight="600">{{.Name}}</text>
      <rect x="200" y="{{.AccBarY}}" width="{{.AccWidth}}" height="14" rx="3" fill="#2563eb"/>
      <text x="{{.AccTextX}}" y="{{.AccTextY}}" font-size="12" fill="#1f2937">{{printf "%.1f%%" .Accuracy}}</text>
      <rect x="200" y="{{.PowBarY}}" width="{{.PowWidth}}" height="14" rx="3" fill="#10b981"/>
      <text x="{{.PowTextX}}" y="{{.PowTextY}}" font-size="12" fill="#1f2937">{{printf "%.1f%%" .PassPowerK}}</text>
      {{end}}
    </svg>
  </div>

  <div class="card">
    <h2>3. 逐题对比明细 (Diff)</h2>
    <div class="toolbar">
      <button id="btn-all" class="active" onclick="filterRows('all')">全部题目 ({{.TotalItems}})</button>
      <button id="btn-diff" onclick="filterRows('diff')">仅看失败或分歧题目</button>
    </div>
    <table>
      <thead>
        <tr>
          <th style="width:220px;">题目与输入预览</th>
          {{range .Targets}}
          <th>{{.Name}}</th>
          {{end}}
        </tr>
      </thead>
      <tbody>
        {{range .Items}}
        <tr class="item-row" data-issue="{{if or .HasDisagreement .HasFailure}}1{{else}}0{{end}}">
          <td>
            <strong>{{.ItemID}}</strong>
            {{if .HasDisagreement}}<span class="badge-warn">存在分歧</span>{{end}}
            <div class="muted">{{.PromptPreview}}</div>
          </td>
          {{range .OrderedResults}}
          <td>
            {{if .PassedAll}}
              <span class="badge-pass">通过 {{.PassCount}}/{{.TrialCount}}</span>
            {{else}}
              <span class="badge-fail">通过 {{.PassCount}}/{{.TrialCount}}</span>
            {{end}}
            <span class="muted">({{.AvgTotalMs}} ms)</span>
            {{if .SampleTools}}<div class="mono">工具: {{.SampleTools}}</div>{{end}}
            {{if .SampleOutput}}<div class="mono">输出: {{.SampleOutput}}</div>{{end}}
            {{if .SampleReason}}<div class="muted">评判: {{.SampleReason}}</div>{{end}}
          </td>
          {{end}}
        </tr>
        {{end}}
      </tbody>
    </table>
  </div>

  <script>
    const EVALREEL_SUMMARY = {{.RawJSON}};
    function filterRows(mode) {
      document.getElementById('btn-all').classList.toggle('active', mode === 'all');
      document.getElementById('btn-diff').classList.toggle('active', mode === 'diff');
      document.querySelectorAll('.item-row').forEach(function(row) {
        if (mode === 'diff' && row.getAttribute('data-issue') !== '1') {
          row.classList.add('hidden');
        } else {
          row.classList.remove('hidden');
        }
      });
    }
  </script>
</body>
</html>`

type svgBar struct {
	Name       string
	Accuracy   float64
	PassPowerK float64
	LabelY     int
	AccBarY    int
	AccWidth   int
	AccTextX   int
	AccTextY   int
	PowBarY    int
	PowWidth   int
	PowTextX   int
	PowTextY   int
}

type htmlView struct {
	Summary
	GeneratedTime string
	SVGHeight     int
	Bars          []svgBar
	RawJSON       template.JS
}

// RenderHTML 生成不依赖任何外部网络脚本的独立单文件 report.html 字符串。
func RenderHTML(summary Summary) (string, error) {
	tmpl, err := template.New("report").Parse(reportHTMLTemplate)
	if err != nil {
		return "", fmt.Errorf("parse html template: %w", err)
	}

	bars := make([]svgBar, 0, len(summary.Targets))
	for i, tgt := range summary.Targets {
		baseY := 40 + i*52
		accW := int(tgt.Accuracy * 4.5)
		powW := int(tgt.PassPowerK * 4.5)
		bars = append(bars, svgBar{
			Name:       tgt.Name,
			Accuracy:   tgt.Accuracy,
			PassPowerK: tgt.PassPowerK,
			LabelY:     baseY + 20,
			AccBarY:    baseY + 4,
			AccWidth:   accW,
			AccTextX:   208 + accW,
			AccTextY:   baseY + 15,
			PowBarY:    baseY + 24,
			PowWidth:   powW,
			PowTextX:   208 + powW,
			PowTextY:   baseY + 35,
		})
	}

	svgH := 50 + len(bars)*52
	if svgH < 90 {
		svgH = 90
	}

	rawJSONBytes, err := json.Marshal(summary)
	if err != nil {
		return "", fmt.Errorf("marshal summary json: %w", err)
	}
	// 转义 </script> 防止嵌入式 JSON 提前闭合标签
	safeJSON := strings.ReplaceAll(string(rawJSONBytes), "</", `<\/`)

	view := htmlView{
		Summary:       summary,
		GeneratedTime: summary.GeneratedAt.Format("2006-01-02 15:04:05 UTC"),
		SVGHeight:     svgH,
		Bars:          bars,
		RawJSON:       template.JS(safeJSON),
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, view); err != nil {
		return "", fmt.Errorf("execute html template: %w", err)
	}
	return buf.String(), nil
}

// WriteHTMLFile 将生成的单文件 HTML 战报写入指定目录。
func WriteHTMLFile(outDir, runID, htmlContent string) (string, error) {
	if outDir == "" {
		outDir = "./reports"
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return "", fmt.Errorf("create report dir %s: %w", outDir, err)
	}
	outPath := filepath.Join(outDir, fmt.Sprintf("report_%s.html", runID))
	if err := os.WriteFile(outPath, []byte(htmlContent), 0o644); err != nil {
		return "", fmt.Errorf("write report %s: %w", outPath, err)
	}
	return outPath, nil
}
