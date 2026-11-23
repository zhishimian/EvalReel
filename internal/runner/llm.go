package runner

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"evalreel/internal/config"
	"evalreel/internal/model"
	"evalreel/internal/scheduler"
	"evalreel/internal/sse"
)

// LLMRunner 通过 OpenAI 兼容流式接口并发请求大模型，复用 v1.0 的 sse.ParseStream 解析指标。
type LLMRunner struct {
	transport      *http.Transport
	client         *http.Client
	estimateTokens func(string) int
	seq            atomic.Uint64
}

// NewLLMRunner 创建支持连接复用与关闭空闲连接的流式模型执行器。
func NewLLMRunner() *LLMRunner {
	tr := &http.Transport{
		DisableCompression:  true,
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 100,
		IdleConnTimeout:     30 * time.Second,
	}
	return &LLMRunner{
		transport:      tr,
		client:         &http.Client{Transport: tr},
		estimateTokens: sse.HeuristicEstimateTokens,
	}
}

// CloseIdleConnections 清理连接池里的空闲 TCP 连接，防止测试结束留存协程。
func (r *LLMRunner) CloseIdleConnections() {
	if r.transport != nil {
		r.transport.CloseIdleConnections()
	}
}

// Execute 向指定目标发送一次流式对话请求并提取思考、回答、工具调用和费用。
func (r *LLMRunner) Execute(ctx context.Context, target config.TargetConfig, item DatasetItem) (RunOutput, error) {
	reqBody, reqText, err := buildChatPayload(target.Model, item)
	if err != nil {
		return RunOutput{}, err
	}

	endpoint := strings.TrimRight(target.BaseURL, "/") + "/chat/completions"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(reqBody))
	if err != nil {
		return RunOutput{}, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	apiKey := target.APIKey
	if apiKey == "" && target.APIKeyEnv != "" {
		apiKey = os.Getenv(target.APIKeyEnv)
	}
	if apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+apiKey)
	}

	start := time.Now()
	resp, err := r.client.Do(req)
	if err != nil {
		if ctx.Err() == nil {
			return RunOutput{}, scheduler.NewRetryableError(err, 0)
		}
		return RunOutput{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusTooManyRequests {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		retryAfter := parseRetryAfter(resp.Header.Get("Retry-After"))
		return RunOutput{}, scheduler.NewRetryableError(
			fmt.Errorf("http 429: %s", strings.TrimSpace(string(snippet))),
			retryAfter,
		)
	}
	if resp.StatusCode >= 500 {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return RunOutput{}, scheduler.NewRetryableError(
			fmt.Errorf("http %d: %s", resp.StatusCode, strings.TrimSpace(string(snippet))),
			0,
		)
	}
	if resp.StatusCode >= 400 {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return RunOutput{}, fmt.Errorf("http %d: %s", resp.StatusCode, strings.TrimSpace(string(snippet)))
	}

	res := sse.ParseStream(resp.Body, sse.ParseOptions{
		StartTime:      start,
		RequestText:    reqText,
		EstimateTokens: r.estimateTokens,
	})

	ttftMs := res.Metrics.TTFTAnswerMs
	if ttftMs == 0 {
		ttftMs = res.Metrics.TTFTReasoningMs
	}

	costUSD := (float64(res.Metrics.InputTokens)/1_000_000.0)*target.InputPricePer1M +
		(float64(res.Metrics.OutputTokens)/1_000_000.0)*target.OutputPricePer1M

	out := RunOutput{
		TraceID:       fmt.Sprintf("run-%d-%d", start.UnixNano(), r.seq.Add(1)),
		ReasoningText: res.ReasoningText,
		ResponseText:  res.ContentText,
		ToolCalls:     res.ToolCalls,
		FinishReason:  res.FinishReason,
		TTFTMs:        ttftMs,
		TotalMs:       res.Metrics.TotalMs,
		InputTokens:   res.Metrics.InputTokens,
		OutputTokens:  res.Metrics.OutputTokens,
		CostUSD:       costUSD,
	}

	if res.Status == model.StatusError {
		return out, fmt.Errorf("stream error: %s", res.RawError)
	}
	return out, nil
}

func buildChatPayload(modelName string, item DatasetItem) ([]byte, string, error) {
	payload := map[string]any{
		"model":  modelName,
		"stream": true,
		"stream_options": map[string]any{
			"include_usage": true,
		},
	}

	var reqText string
	if len(item.Messages) > 0 && string(item.Messages) != "null" {
		payload["messages"] = item.Messages
		reqText = string(item.Messages)
	} else {
		userPrompt := item.Prompt
		if item.EntryPoint != "" {
			userPrompt = "Complete the following Python function directly in a ```python code block without extra explanation:\n\n" + item.Prompt
		}
		payload["messages"] = []map[string]string{
			{"role": "user", "content": userPrompt},
		}
		reqText = userPrompt
	}

	if len(item.Tools) > 0 && string(item.Tools) != "null" {
		payload["tools"] = item.Tools
	}

	b, err := json.Marshal(payload)
	if err != nil {
		return nil, "", fmt.Errorf("marshal chat payload: %w", err)
	}
	return b, reqText, nil
}

func parseRetryAfter(raw string) time.Duration {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0
	}
	sec, err := strconv.Atoi(raw)
	if err != nil || sec <= 0 {
		return 0
	}
	return time.Duration(sec) * time.Second
}
