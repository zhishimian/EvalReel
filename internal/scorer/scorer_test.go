package scorer

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"evalreel/internal/config"
	"evalreel/internal/model"
	"evalreel/internal/runner"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestRuleScorer_ToolCallAndNegativeCase(t *testing.T) {
	items, err := runner.LoadDataset(filepath.Join("..", "..", "testdata", "toolcall_mini.jsonl"))
	if err != nil {
		t.Fatalf("LoadDataset: %v", err)
	}
	rs, err := NewRuleScorer(config.ScorerConfig{Type: "rule", Name: "tool_match"})
	if err != nil {
		t.Fatalf("NewRuleScorer: %v", err)
	}

	// 1. tc-01 正向匹配成功
	scPass, err := rs.Score(context.Background(), items[0], runner.RunOutput{
		ToolCalls: []model.ToolCall{{Name: "get_weather", Arguments: `{"city":"北京","unit":"celsius"}`}},
	})
	if err != nil || !scPass.Passed {
		t.Fatalf("expected tc-01 pass, got %+v err=%v", scPass, err)
	}

	// 2. 参数不匹配失败
	scFail, err := rs.Score(context.Background(), items[0], runner.RunOutput{
		ToolCalls: []model.ToolCall{{Name: "get_weather", Arguments: `{"city":"上海"}`}},
	})
	if err != nil || scFail.Passed {
		t.Fatalf("expected wrong city fail, got %+v err=%v", scFail, err)
	}

	// 3. tc-10 反例题（不应调用工具）：没调工具则过，乱调工具则判错
	negPass, err := rs.Score(context.Background(), items[9], runner.RunOutput{
		ResponseText: "HTTP 429 表示请求过于频繁。",
	})
	if err != nil || !negPass.Passed {
		t.Fatalf("expected negative case pass without tool calls, got %+v err=%v", negPass, err)
	}

	negFail, err := rs.Score(context.Background(), items[9], runner.RunOutput{
		ToolCalls: []model.ToolCall{{Name: "get_weather", Arguments: `{"city":"北京"}`}},
	})
	if err != nil || negFail.Passed {
		t.Fatalf("expected negative case fail when tool called, got %+v err=%v", negFail, err)
	}
}

func TestRuleScorer_Regex(t *testing.T) {
	rs, err := NewRuleScorer(config.ScorerConfig{
		Type:    "rule",
		Name:    "regex_check",
		Pattern: `1067\.\d+`,
	})
	if err != nil {
		t.Fatalf("NewRuleScorer: %v", err)
	}

	pass, _ := rs.Score(context.Background(), runner.DatasetItem{ID: "r1"}, runner.RunOutput{ResponseText: "距离约 1067.31 公里"})
	fail, _ := rs.Score(context.Background(), runner.DatasetItem{ID: "r2"}, runner.RunOutput{ResponseText: "未知距离"})
	if !pass.Passed || fail.Passed {
		t.Fatalf("unexpected regex results: pass=%v fail=%v", pass.Passed, fail.Passed)
	}
}

func TestJudgeScorer_BinaryPassFail(t *testing.T) {
	callIdx := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		callIdx++
		verdict := "```json\n{\"checklist\":[{\"item\":\"准确性\",\"ok\":true}],\"reason\":\"回答正确\",\"passed\":true}\n```"
		if callIdx == 2 {
			verdict = `{"checklist":[{"item":"准确性","ok":false}],"reason":"漏掉边界条件","passed":false}`
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"choices": []map[string]any{
				{"message": map[string]any{"content": verdict}},
			},
		})
	}))
	defer srv.Close()

	js, err := NewJudgeScorer(config.ScorerConfig{
		Type:         "judge",
		Name:         "judge_v1",
		JudgeBaseURL: srv.URL + "/v1",
		JudgeModel:   "judge-model",
		Rubric:       "检查是否准确且覆盖边界情况",
	})
	if err != nil {
		t.Fatalf("NewJudgeScorer: %v", err)
	}
	defer js.CloseIdleConnections()

	s1, err := js.Score(context.Background(), runner.DatasetItem{ID: "j1", Prompt: "Q1"}, runner.RunOutput{ResponseText: "A1"})
	if err != nil || !s1.Passed || s1.RubricHash == "" || s1.Reason != "回答正确" {
		t.Fatalf("unexpected judge pass result: %+v err=%v", s1, err)
	}

	s2, err := js.Score(context.Background(), runner.DatasetItem{ID: "j2", Prompt: "Q2"}, runner.RunOutput{ResponseText: "A2"})
	if err != nil || s2.Passed || !strings.Contains(s2.Reason, "边界") {
		t.Fatalf("unexpected judge fail result: %+v err=%v", s2, err)
	}
}

func TestExecScorer_HumanEvalPassAndFail(t *testing.T) {
	items, err := runner.LoadDataset(filepath.Join("..", "..", "testdata", "humaneval_mini.jsonl"))
	if err != nil {
		t.Fatalf("LoadDataset: %v", err)
	}
	isPal := items[3] // he-04: is_palindrome

	es := NewExecScorer(config.ScorerConfig{Type: "exec", Name: "exec_py"})

	goodOut := runner.RunOutput{
		ResponseText: "```python\ndef is_palindrome(text: str) -> bool:\n    return text == text[::-1]\n```",
	}
	passScore, err := es.Score(context.Background(), isPal, goodOut)
	if err != nil || !passScore.Passed {
		t.Fatalf("expected HumanEval pass, got %+v err=%v", passScore, err)
	}

	badOut := runner.RunOutput{
		ResponseText: "```python\ndef is_palindrome(text: str) -> bool:\n    return True\n```",
	}
	failScore, err := es.Score(context.Background(), isPal, badOut)
	if err != nil || failScore.Passed {
		t.Fatalf("expected HumanEval fail on buggy code, got %+v err=%v", failScore, err)
	}
}

func TestExecScorer_WorkDirCheck(t *testing.T) {
	workDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(workDir, "math_utils.py"), []byte("def add(a, b):\n    return a + b\n"), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}

	es := NewExecScorer(config.ScorerConfig{
		Type:    "exec",
		Name:    "workdir_py",
		Command: "python",
		Args:    []string{"-c", "import math_utils; assert math_utils.add(2, 3) == 5"},
	})

	sc, err := es.Score(context.Background(), runner.DatasetItem{ID: "wd-1"}, runner.RunOutput{WorkDir: workDir})
	if err != nil || !sc.Passed {
		t.Fatalf("expected workdir check pass, got %+v err=%v", sc, err)
	}
}
