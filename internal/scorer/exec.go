package scorer

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"evalreel/internal/config"
	"evalreel/internal/runner"
	"evalreel/internal/store"
)

// ExecScorer 通过运行外部测试脚本或 Python 单元测试并检查退出码（0 为通过）来客观判分。
type ExecScorer struct {
	name    string
	command string
	args    []string
	timeout time.Duration
}

// NewExecScorer 创建外部命令与代码单元测试判分器。
func NewExecScorer(cfg config.ScorerConfig) *ExecScorer {
	name := cfg.Name
	if name == "" {
		name = "exec"
	}
	return &ExecScorer{
		name:    name,
		command: cfg.Command,
		args:    cfg.Args,
		timeout: 15 * time.Second,
	}
}

// Name 返回批卷器名称。
func (e *ExecScorer) Name() string {
	return e.name
}

// Score 若题目自带单元测试（如 HumanEval），自动拼接并运行 Python 校验；否则在工作目录执行自定义校验命令。
func (e *ExecScorer) Score(ctx context.Context, item runner.DatasetItem, out runner.RunOutput) (store.EvalScore, error) {
	score := store.EvalScore{
		ScorerName: e.name,
	}

	if item.Test != "" {
		return e.gradePythonUnitTest(ctx, item, out, score)
	}
	if e.command == "" {
		return score, fmt.Errorf("exec scorer requires either item.test or scorer.command")
	}
	return e.gradeCustomCommand(ctx, out, score)
}

func (e *ExecScorer) gradePythonUnitTest(ctx context.Context, item runner.DatasetItem, out runner.RunOutput, score store.EvalScore) (store.EvalScore, error) {
	code := extractPythonCode(out.ResponseText)
	if code == "" {
		score.Passed = false
		score.Reason = "no python code produced"
		return score, nil
	}

	if item.EntryPoint != "" && !strings.Contains(code, "def "+item.EntryPoint) {
		code = item.Prompt + "\n" + code
	} else {
		code = prependMissingImports(item.Prompt, code)
	}

	fullScript := code + "\n\n" + item.Test + "\n"
	tmpFile, err := os.CreateTemp("", "evalreel-test-*.py")
	if err != nil {
		return score, fmt.Errorf("create temp test file: %w", err)
	}
	tmpPath := tmpFile.Name()
	if _, wErr := tmpFile.WriteString(fullScript); wErr != nil {
		_ = tmpFile.Close()
		_ = os.Remove(tmpPath)
		return score, wErr
	}
	_ = tmpFile.Close()
	defer os.Remove(tmpPath)

	pyCmd := e.command
	if pyCmd == "" {
		pyCmd = "python"
	}

	runCtx, cancel := context.WithTimeout(ctx, e.timeout)
	defer cancel()

	cmd := exec.CommandContext(runCtx, pyCmd, tmpPath)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		score.Passed = false
		msg := strings.TrimSpace(stderr.String())
		if msg == "" {
			msg = err.Error()
		}
		score.Reason = msg
		return score, nil
	}

	score.Passed = true
	score.Reason = "python unit tests passed (exit code 0)"
	return score, nil
}

func (e *ExecScorer) gradeCustomCommand(ctx context.Context, out runner.RunOutput, score store.EvalScore) (store.EvalScore, error) {
	runCtx, cancel := context.WithTimeout(ctx, e.timeout)
	defer cancel()

	cmd := exec.CommandContext(runCtx, e.command, e.args...)
	if out.WorkDir != "" {
		cmd.Dir = out.WorkDir
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		score.Passed = false
		msg := strings.TrimSpace(stderr.String())
		if msg == "" {
			msg = err.Error()
		}
		score.Reason = msg
		return score, nil
	}

	score.Passed = true
	score.Reason = "custom command passed (exit code 0)"
	return score, nil
}

func extractPythonCode(raw string) string {
	s := strings.TrimSpace(raw)
	if idx := strings.Index(s, "```python"); idx >= 0 {
		rest := s[idx+len("```python"):]
		if end := strings.Index(rest, "```"); end >= 0 {
			return strings.TrimSpace(rest[:end])
		}
	}
	if idx := strings.Index(s, "```"); idx >= 0 {
		rest := s[idx+3:]
		if nl := strings.IndexByte(rest, '\n'); nl >= 0 {
			rest = rest[nl+1:]
		}
		if end := strings.Index(rest, "```"); end >= 0 {
			return strings.TrimSpace(rest[:end])
		}
	}
	return s
}

func prependMissingImports(prompt, code string) string {
	var missing []string
	for _, line := range strings.Split(prompt, "\n") {
		trimmed := strings.TrimSpace(line)
		if (strings.HasPrefix(trimmed, "from ") || strings.HasPrefix(trimmed, "import ")) && !strings.Contains(code, trimmed) {
			missing = append(missing, trimmed)
		}
	}
	if len(missing) == 0 {
		return code
	}
	return strings.Join(missing, "\n") + "\n\n" + code
}
