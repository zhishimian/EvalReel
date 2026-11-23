package runner

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"evalreel/internal/config"
)

// AgentRunner 为每次运行分配独立临时目录，拉起外部 Agent 子进程并在超时时强杀进程树。
type AgentRunner struct {
	AgentCfg config.AgentConfig
}

// NewAgentRunner 创建一个隔离子进程 Agent 执行器。
func NewAgentRunner(cfg config.AgentConfig) *AgentRunner {
	return &AgentRunner{AgentCfg: cfg}
}

// Execute 在独立临时目录运行外部 Agent 命令；返回的 WorkDir 供后续批卷检查生成文件。
func (r *AgentRunner) Execute(ctx context.Context, target config.TargetConfig, item DatasetItem) (RunOutput, error) {
	if r.AgentCfg.Command == "" {
		return RunOutput{}, fmt.Errorf("agent command is empty")
	}

	workDir, err := os.MkdirTemp("", "evalreel-agent-*")
	if err != nil {
		return RunOutput{}, fmt.Errorf("create isolated workdir: %w", err)
	}

	args := append([]string(nil), r.AgentCfg.Args...)
	if item.Prompt != "" {
		args = append(args, item.Prompt)
	}

	runCtx := ctx
	var cancel context.CancelFunc
	if r.AgentCfg.TimeoutSec > 0 {
		runCtx, cancel = context.WithTimeout(ctx, time.Duration(r.AgentCfg.TimeoutSec)*time.Second)
		defer cancel()
	}

	cmd := exec.CommandContext(runCtx, r.AgentCfg.Command, args...)
	cmd.Dir = workDir
	configureProcessGroup(cmd)
	cmd.Cancel = func() error {
		return killProcessTree(cmd)
	}
	cmd.WaitDelay = 2 * time.Second

	env := append([]string(nil), os.Environ()...)
	apiKey := target.APIKey
	if apiKey == "" && target.APIKeyEnv != "" {
		apiKey = os.Getenv(target.APIKeyEnv)
	}
	if target.BaseURL != "" {
		env = append(env, "OPENAI_BASE_URL="+target.BaseURL, "EVALREEL_PROXY_URL="+target.BaseURL)
	}
	if apiKey != "" {
		env = append(env, "OPENAI_API_KEY="+apiKey)
	}
	if target.Model != "" {
		env = append(env, "OPENAI_MODEL="+target.Model)
	}
	cmd.Env = env

	var stdoutBuf, stderrBuf bytes.Buffer
	cmd.Stdout = &stdoutBuf
	cmd.Stderr = &stderrBuf

	start := time.Now()
	err = cmd.Run()
	elapsedMs := time.Since(start).Milliseconds()

	outText := strings.TrimSpace(stdoutBuf.String())
	if err != nil {
		errDetail := strings.TrimSpace(stderrBuf.String())
		if errDetail != "" {
			return RunOutput{ResponseText: outText, TotalMs: elapsedMs, WorkDir: workDir},
				fmt.Errorf("agent failed (%v): %s", err, errDetail)
		}
		return RunOutput{ResponseText: outText, TotalMs: elapsedMs, WorkDir: workDir},
			fmt.Errorf("agent failed: %w", err)
	}

	return RunOutput{
		ResponseText: outText,
		FinishReason: "stop",
		TotalMs:      elapsedMs,
		WorkDir:      workDir,
	}, nil
}
