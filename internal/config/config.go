package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config 保存代理服务、上游地址、轨迹落盘和混沌故障规则的全部配置。
type Config struct {
	Server   ServerConfig   `yaml:"server"`
	Upstream UpstreamConfig `yaml:"upstream"`
	Recorder RecorderConfig `yaml:"recorder"`
	Chaos    ChaosConfig    `yaml:"chaos"`
}

// ServerConfig 配置代理服务器监听端口和优雅退出时间。
type ServerConfig struct {
	Listen          string `yaml:"listen"`
	PProfListen     string `yaml:"pprof_listen"`
	ShutdownTimeout string `yaml:"shutdown_timeout"`
}

// UpstreamConfig 配置真实大模型或模拟服务上游地址与请求超时。
type UpstreamConfig struct {
	BaseURL string `yaml:"base_url"`
	Timeout string `yaml:"timeout"`
}

// RecorderConfig 配置轨迹保存目录、会话编号及允许缓存的最大请求体大小。
type RecorderConfig struct {
	OutputDir         string `yaml:"output_dir"`
	SessionID         string `yaml:"session_id"`
	MaxRequestBodyMB  int    `yaml:"max_request_body_mb"`
}

// ChaosConfig 控制是否开启故障注入及全部注入规则列表。
type ChaosConfig struct {
	Enabled bool        `yaml:"enabled"`
	Rules   []ChaosRule `yaml:"rules"`
}

// ChaosRule 描述单条混沌故障的匹配路径、触发频率和具体故障动作。
type ChaosRule struct {
	Name           string  `yaml:"name"`
	MatchPath      string  `yaml:"match_path"`
	EveryN         int64   `yaml:"every_n"`
	Probability    float64 `yaml:"probability"`
	Action         string  `yaml:"action"` // delay | status_429 | status_500 | cut_stream | malformed_json
	DelayMs        int     `yaml:"delay_ms"`
	RetryAfterSec  int     `yaml:"retry_after_sec"`
	CutAfterChunks int     `yaml:"cut_after_chunks"`
}

// DefaultConfig 返回本地调试开箱即用的默认配置（默认指向本地 mockllm）。
func DefaultConfig() *Config {
	return &Config{
		Server: ServerConfig{
			Listen:          "127.0.0.1:8080",
			PProfListen:     "127.0.0.1:6060",
			ShutdownTimeout: "15s",
		},
		Upstream: UpstreamConfig{
			BaseURL: "http://127.0.0.1:9090",
			Timeout: "120s",
		},
		Recorder: RecorderConfig{
			OutputDir:        "./traces",
			MaxRequestBodyMB: 10,
		},
		Chaos: ChaosConfig{
			Enabled: false,
		},
	}
}

// Load 从 YAML 文件读取配置并填充缺省默认值。
func Load(path string) (*Config, error) {
	cfg := DefaultConfig()
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse yaml %s: %w", path, err)
	}
	cfg.ApplyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// ApplyDefaults 为空字段补齐安全默认值。
func (c *Config) ApplyDefaults() {
	def := DefaultConfig()
	if c.Server.Listen == "" {
		c.Server.Listen = def.Server.Listen
	}
	if c.Server.ShutdownTimeout == "" {
		c.Server.ShutdownTimeout = def.Server.ShutdownTimeout
	}
	if c.Upstream.BaseURL == "" {
		c.Upstream.BaseURL = def.Upstream.BaseURL
	}
	if c.Upstream.Timeout == "" {
		c.Upstream.Timeout = def.Upstream.Timeout
	}
	if c.Recorder.OutputDir == "" {
		c.Recorder.OutputDir = def.Recorder.OutputDir
	}
	if c.Recorder.MaxRequestBodyMB <= 0 {
		c.Recorder.MaxRequestBodyMB = def.Recorder.MaxRequestBodyMB
	}
}

// Validate 校验时间字符串和故障规则参数合法性。
func (c *Config) Validate() error {
	if _, err := c.ParseShutdownTimeout(); err != nil {
		return fmt.Errorf("invalid server.shutdown_timeout: %w", err)
	}
	if _, err := c.ParseUpstreamTimeout(); err != nil {
		return fmt.Errorf("invalid upstream.timeout: %w", err)
	}
	return nil
}

// ParseShutdownTimeout 将优雅关闭超时配置解析为 time.Duration。
func (c *Config) ParseShutdownTimeout() (time.Duration, error) {
	return time.ParseDuration(c.Server.ShutdownTimeout)
}

// ParseUpstreamTimeout 将上游请求超时配置解析为 time.Duration。
func (c *Config) ParseUpstreamTimeout() (time.Duration, error) {
	return time.ParseDuration(c.Upstream.Timeout)
}
