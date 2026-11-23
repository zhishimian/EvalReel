package scorer

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"evalreel/internal/config"
	"evalreel/internal/runner"
	"evalreel/internal/store"
)

// RuleScorer 负责零成本的确定性校验：正则表达式匹配与工具调用名称/参数核对。
type RuleScorer struct {
	name    string
	pattern *regexp.Regexp
}

type toolExpected struct {
	ToolName     string         `json:"tool_name"`
	RequiredArgs map[string]any `json:"required_args"`
	MustNotCall  bool           `json:"must_not_call"`
}

// NewRuleScorer 创建规则批卷器。
func NewRuleScorer(cfg config.ScorerConfig) (*RuleScorer, error) {
	name := cfg.Name
	if name == "" {
		name = "rule"
	}
	var re *regexp.Regexp
	if cfg.Pattern != "" {
		var err error
		re, err = regexp.Compile(cfg.Pattern)
		if err != nil {
			return nil, fmt.Errorf("compile regex %q: %w", cfg.Pattern, err)
		}
	}
	return &RuleScorer{
		name:    name,
		pattern: re,
	}, nil
}

// Name 返回批卷器名称。
func (r *RuleScorer) Name() string {
	return r.name
}

// Score 按工具调用期望和正则规则判定通过与否，不消耗额外模型费用。
func (r *RuleScorer) Score(_ context.Context, item runner.DatasetItem, out runner.RunOutput) (store.EvalScore, error) {
	score := store.EvalScore{
		ScorerName: r.name,
	}

	if len(item.Expected) > 0 && string(item.Expected) != "null" {
		var exp toolExpected
		if err := json.Unmarshal(item.Expected, &exp); err != nil {
			return score, fmt.Errorf("parse item.expected: %w", err)
		}
		passed, reason, details := checkToolExpectation(exp, out)
		score.Passed = passed
		score.Reason = reason
		score.DetailsJSON = details
		return score, nil
	}

	if r.pattern != nil {
		if r.pattern.MatchString(out.ResponseText) {
			score.Passed = true
			score.Reason = "matched regex pattern"
		} else {
			score.Passed = false
			score.Reason = fmt.Sprintf("response did not match pattern %q", r.pattern.String())
		}
		return score, nil
	}

	if strings.TrimSpace(out.ResponseText) == "" && len(out.ToolCalls) == 0 {
		score.Passed = false
		score.Reason = "empty model output"
		return score, nil
	}
	score.Passed = true
	score.Reason = "non-empty output"
	return score, nil
}

func checkToolExpectation(exp toolExpected, out runner.RunOutput) (bool, string, string) {
	if exp.MustNotCall {
		if len(out.ToolCalls) == 0 {
			return true, "correctly answered without calling tools", `{"tool_calls":0}`
		}
		names := make([]string, 0, len(out.ToolCalls))
		for _, tc := range out.ToolCalls {
			names = append(names, tc.Name)
		}
		return false, fmt.Sprintf("unexpected tool calls: %v", names), `{"must_not_call_violated":true}`
	}

	if len(out.ToolCalls) == 0 {
		return false, fmt.Sprintf("expected tool %q, but no tool was called", exp.ToolName), `{}`
	}

	for _, tc := range out.ToolCalls {
		if exp.ToolName != "" && tc.Name != exp.ToolName {
			continue
		}
		var gotArgs map[string]any
		if err := json.Unmarshal([]byte(tc.Arguments), &gotArgs); err != nil {
			return false, fmt.Sprintf("tool %q returned invalid json args: %v", tc.Name, err), `{}`
		}
		for k, wantVal := range exp.RequiredArgs {
			gotVal, ok := gotArgs[k]
			if !ok {
				return false, fmt.Sprintf("tool %q missing required arg %q", tc.Name, k), tc.Arguments
			}
			if !valuesEqual(gotVal, wantVal) {
				return false, fmt.Sprintf("tool %q arg %q=%v, want %v", tc.Name, k, gotVal, wantVal), tc.Arguments
			}
		}
		return true, fmt.Sprintf("tool %q and required args matched", tc.Name), tc.Arguments
	}

	return false, fmt.Sprintf("expected tool %q, got %q", exp.ToolName, out.ToolCalls[0].Name), `{}`
}

func valuesEqual(a, b any) bool {
	if fa, okA := toFloat64(a); okA {
		if fb, okB := toFloat64(b); okB {
			return fa == fb
		}
	}
	return fmt.Sprint(a) == fmt.Sprint(b)
}

func toFloat64(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	default:
		return 0, false
	}
}
