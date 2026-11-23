package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
)

// EvalConfig 描述单场 v2.0 评测任务的完整 JSON 配置。
type EvalConfig struct {
	Name           string           `json:"name"`
	Mode           string           `json:"mode"` // benchmark | fork | agent
	DatasetPath    string           `json:"dataset_path"`
	ForkSource     ForkSourceConfig `json:"fork_source"`
	Agent          AgentConfig      `json:"agent"`
	Concurrency    int              `json:"concurrency"`
	Trials         int              `json:"trials"`
	MaxRetries     int              `json:"max_retries"`
	TaskTimeoutSec int              `json:"task_timeout_sec"`
	Targets        []TargetConfig   `json:"targets"`
	Scorers        []ScorerConfig   `json:"scorers"`
	OutputDir      string           `json:"output_dir"`
}

// ForkSourceConfig 指定第 N 步读档分叉测试所读取的历史轨迹文件与切入步数。
type ForkSourceConfig struct {
	TraceFile string `json:"trace_file"`
	TraceID   string `json:"trace_id"`
	StepIndex int    `json:"step_index"`
}

// AgentConfig 配置隔离子进程模式下要拉起的外部 Agent 命令。
type AgentConfig struct {
	Command    string   `json:"command"`
	Args       []string `json:"args"`
	TimeoutSec int      `json:"timeout_sec"`
}

// TargetConfig 描述一个待测大模型或接口目标的地址、限流与单价信息。
type TargetConfig struct {
	Name              string  `json:"name"`
	BaseURL           string  `json:"base_url"`
	APIKeyEnv         string  `json:"api_key_env"`
	APIKey            string  `json:"api_key,omitempty"`
	Model             string  `json:"model"`
	RPM               int     `json:"rpm_limit"`
	InputPricePer1M   float64 `json:"input_price_per_1m"`
	OutputPricePer1M  float64 `json:"output_price_per_1m"`
}

// ScorerConfig 描述单个自动批卷器的类型与评判参数。
type ScorerConfig struct {
	Type           string   `json:"type"` // rule | judge | exec
	Name           string   `json:"name"`
	Pattern        string   `json:"pattern,omitempty"`
	JudgeModel     string   `json:"judge_model,omitempty"`
	JudgeBaseURL   string   `json:"judge_base_url,omitempty"`
	JudgeAPIKeyEnv string   `json:"judge_api_key_env,omitempty"`
	Rubric         string   `json:"rubric,omitempty"`
	Command        string   `json:"command,omitempty"`
	Args           []string `json:"args,omitempty"`
}

// LoadEvalConfig 从 JSON 文件加载评测配置并执行默认值填充与校验。
func LoadEvalConfig(path string) (*EvalConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read eval config %s: %w", path, err)
	}
	var cfg EvalConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse eval config %s: %w", path, err)
	}
	cfg.ApplyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// ApplyDefaults 为未填写的评测参数填充安全默认值。
func (c *EvalConfig) ApplyDefaults() {
	if c.Mode == "" {
		c.Mode = "benchmark"
	}
	if c.Concurrency <= 0 {
		c.Concurrency = 10
	}
	if c.Trials <= 0 {
		c.Trials = 1
	}
	if c.MaxRetries < 0 {
		c.MaxRetries = 0
	}
	if c.TaskTimeoutSec <= 0 {
		c.TaskTimeoutSec = 120
	}
	if c.OutputDir == "" {
		c.OutputDir = "./reports"
	}
}

// Validate 检查评测模式和被测目标配置是否合法。
func (c *EvalConfig) Validate() error {
	switch c.Mode {
	case "benchmark", "fork", "agent":
	default:
		return fmt.Errorf("unsupported eval mode %q (want benchmark|fork|agent)", c.Mode)
	}
	if len(c.Targets) == 0 {
		return errors.New("at least one target is required")
	}
	for i, tgt := range c.Targets {
		if tgt.Name == "" || tgt.BaseURL == "" || tgt.Model == "" {
			return fmt.Errorf("target[%d] requires name, base_url and model", i)
		}
	}
	if c.Mode == "fork" && c.ForkSource.TraceFile == "" {
		return errors.New("fork mode requires fork_source.trace_file")
	}
	if c.Mode == "agent" && c.Agent.Command == "" {
		return errors.New("agent mode requires agent.command")
	}
	return nil
}
