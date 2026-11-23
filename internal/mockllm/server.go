package mockllm

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Config 定义模拟大模型服务的默认行为参数、流式分块数、随机 429 概率与取消回调。
type Config struct {
	TTFT          time.Duration
	ChunkInterval time.Duration
	Chunks        int     // >0 时按指定块数输出正文流式分块，用于真实慢流式压测
	Fault429Rate  float64 // 0~1 之间，未显式传 X-Mock-Fault 时随机返回 429 的概率
	ModelName     string
	OnCanceled    func()
}

// DefaultConfig 返回贴近真实推理模型的默认模拟配置。
func DefaultConfig() Config {
	return Config{
		TTFT:          300 * time.Millisecond,
		ChunkInterval: 30 * time.Millisecond,
		ModelName:     "mock-reasoning-v1",
	}
}

// Server 提供兼容 OpenAI 协议的模拟接口，用于离线功能测试与压测。
type Server struct {
	cfg Config
}

// NewServer 创建一个模拟大模型服务端实例。
func NewServer(cfg Config) *Server {
	if cfg.ModelName == "" {
		cfg.ModelName = "mock-reasoning-v1"
	}
	return &Server{cfg: cfg}
}

// Handler 返回挂载好路由的 HTTP 处理器。
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/models", s.handleModels)
	mux.HandleFunc("POST /v1/chat/completions", s.handleChatCompletions)
	return mux
}

type chatRequest struct {
	Model         string          `json:"model"`
	Messages      json.RawMessage `json:"messages"`
	Stream        bool            `json:"stream"`
	StreamOptions *struct {
		IncludeUsage bool `json:"include_usage"`
	} `json:"stream_options"`
}

type usagePayload struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

func (s *Server) handleModels(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"object": "list",
		"data": []map[string]any{
			{
				"id":       s.cfg.ModelName,
				"object":   "model",
				"owned_by": "evalreel-mock",
			},
		},
	})
}

func (s *Server) handleChatCompletions(w http.ResponseWriter, r *http.Request) {
	var req chatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid_request_error", "invalid JSON request body")
		return
	}

	modelName := req.Model
	if modelName == "" {
		modelName = s.cfg.ModelName
	}

	ttft := parseDurationHeader(r, "X-Mock-TTFT-Ms", s.cfg.TTFT)
	interval := parseDurationHeader(r, "X-Mock-Chunk-Interval-Ms", s.cfg.ChunkInterval)
	fault := strings.TrimSpace(r.Header.Get("X-Mock-Fault"))
	retryAfter := "2"
	if fault == "" && s.cfg.Fault429Rate > 0 && rand.Float64() < s.cfg.Fault429Rate {
		fault = "429"
		retryAfter = "1"
	}

	switch fault {
	case "429":
		w.Header().Set("Retry-After", retryAfter)
		writeJSONError(w, http.StatusTooManyRequests, "rate_limit_exceeded", "mock rate limit triggered")
		return
	case "500":
		writeJSONError(w, http.StatusInternalServerError, "server_error", "mock internal server error")
		return
	}

	// 首字等待阶段必须监听请求上下文取消信号，确保客户端断开后立刻退出。
	if !sleepWithContext(r.Context(), ttft) {
		s.notifyCanceled()
		return
	}

	if !req.Stream {
		s.writeNonStreamResponse(w, modelName)
		return
	}

	includeUsage := req.StreamOptions != nil && req.StreamOptions.IncludeUsage
	s.writeStreamResponse(w, r, modelName, interval, fault, includeUsage)
}

func (s *Server) writeNonStreamResponse(w http.ResponseWriter, modelName string) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"id":      "chatcmpl-mock-001",
		"object":  "chat.completion",
		"created": time.Now().Unix(),
		"model":   modelName,
		"choices": []map[string]any{
			{
				"index": 0,
				"message": map[string]any{
					"role":              "assistant",
					"reasoning_content": "先分析用户问题，再整理最终回答。",
					"content":           "这是来自 EvalReel 模拟服务的非流式回答。",
				},
				"finish_reason": "stop",
			},
		},
		"usage": usagePayload{
			PromptTokens:     18,
			CompletionTokens: 24,
			TotalTokens:      42,
		},
	})
}

func (s *Server) writeStreamResponse(w http.ResponseWriter, r *http.Request, modelName string, interval time.Duration, fault string, includeUsage bool) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)

	flusher, canFlush := w.(http.Flusher)
	if canFlush {
		flusher.Flush()
	}

	sendChunk := func(payload any) bool {
		if !writeSSEJSON(w, payload) {
			return false
		}
		if canFlush {
			flusher.Flush()
		}
		if !sleepWithContext(r.Context(), interval) {
			s.notifyCanceled()
			return false
		}
		return true
	}

	switch fault {
	case "tool_calls":
		s.streamFragmentedToolCalls(modelName, sendChunk)
	case "oversized_chunk":
		bigText := strings.Repeat("a", 130*1024)
		if !sendChunk(makeDeltaChunk(modelName, map[string]any{"content": bigText}, nil)) {
			return
		}
	case "cut_after_3":
		for i := 1; i <= 3; i++ {
			if !sendChunk(makeDeltaChunk(modelName, map[string]any{"content": fmt.Sprintf("片段%d ", i)}, nil)) {
				return
			}
		}
		if hj, ok := w.(http.Hijacker); ok {
			if conn, _, err := hj.Hijack(); err == nil {
				_ = conn.Close()
			}
		}
		return
	case "malformed_json":
		for i := 1; i <= 2; i++ {
			if !sendChunk(makeDeltaChunk(modelName, map[string]any{"content": fmt.Sprintf("正常%d ", i)}, nil)) {
				return
			}
		}
		_, _ = fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\n\n")
		if canFlush {
			flusher.Flush()
		}
		return
	case "mid_stream_error":
		for i := 1; i <= 2; i++ {
			if !sendChunk(makeDeltaChunk(modelName, map[string]any{"content": fmt.Sprintf("正常%d ", i)}, nil)) {
				return
			}
		}
		if !sendChunk(map[string]any{
			"error": map[string]any{
				"message": "upstream overloaded mid-stream",
				"type":    "server_error",
			},
		}) {
			return
		}
	default:
		if s.cfg.Chunks > 0 {
			if !sendChunk(makeDeltaChunk(modelName, map[string]any{"reasoning_content": "先分析条件，"}, nil)) {
				return
			}
			for i := 1; i <= s.cfg.Chunks; i++ {
				if !sendChunk(makeDeltaChunk(modelName, map[string]any{"content": fmt.Sprintf("分片%d ", i)}, nil)) {
					return
				}
			}
		} else {
			reasoningParts := []string{"先确认输入条件，", "再组织清晰步骤。"}
			contentParts := []string{"你好，", "这里是模拟流式响应，", "用于测试延迟与解析。"}
			for _, part := range reasoningParts {
				if !sendChunk(makeDeltaChunk(modelName, map[string]any{"reasoning_content": part}, nil)) {
					return
				}
			}
			for _, part := range contentParts {
				if !sendChunk(makeDeltaChunk(modelName, map[string]any{"content": part}, nil)) {
					return
				}
			}
		}
	}

	stop := "stop"
	if fault == "tool_calls" {
		stop = "tool_calls"
	}
	if !sendChunk(makeDeltaChunk(modelName, map[string]any{}, &stop)) {
		return
	}

	if includeUsage {
		if !sendChunk(map[string]any{
			"id":      "chatcmpl-mock-stream",
			"object":  "chat.completion.chunk",
			"created": time.Now().Unix(),
			"model":   modelName,
			"choices": []any{},
			"usage": usagePayload{
				PromptTokens:     20,
				CompletionTokens: 32,
				TotalTokens:      52,
			},
		}) {
			return
		}
	}

	if fault != "no_done" {
		_, _ = fmt.Fprint(w, "data: [DONE]\n\n")
		if canFlush {
			flusher.Flush()
		}
	}
}

func (s *Server) streamFragmentedToolCalls(modelName string, sendChunk func(any) bool) {
	fragments := []map[string]any{
		{
			"index": 0,
			"id":    "call_mock_1",
			"type":  "function",
			"function": map[string]any{
				"name":      "get_weather",
				"arguments": `{"city":`,
			},
		},
		{"index": 0, "function": map[string]any{"arguments": `"bei`}},
		{"index": 0, "function": map[string]any{"arguments": `jing",`}},
		{"index": 0, "function": map[string]any{"arguments": `"unit":`}},
		{"index": 0, "function": map[string]any{"arguments": `"celsius"}`}},
	}
	for _, frag := range fragments {
		if !sendChunk(makeDeltaChunk(modelName, map[string]any{"tool_calls": []any{frag}}, nil)) {
			return
		}
	}
}

func (s *Server) notifyCanceled() {
	if s.cfg.OnCanceled != nil {
		s.cfg.OnCanceled()
	}
}

func makeDeltaChunk(modelName string, delta map[string]any, finishReason *string) map[string]any {
	choice := map[string]any{
		"index": 0,
		"delta": delta,
	}
	if finishReason != nil {
		choice["finish_reason"] = *finishReason
	} else {
		choice["finish_reason"] = nil
	}
	return map[string]any{
		"id":      "chatcmpl-mock-stream",
		"object":  "chat.completion.chunk",
		"created": time.Now().Unix(),
		"model":   modelName,
		"choices": []map[string]any{choice},
	}
}

func writeSSEJSON(w http.ResponseWriter, payload any) bool {
	b, err := json.Marshal(payload)
	if err != nil {
		return false
	}
	_, err = fmt.Fprintf(w, "data: %s\n\n", b)
	return err == nil
}

func writeJSONError(w http.ResponseWriter, status int, errType, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{
			"message": message,
			"type":    errType,
		},
	})
}

func parseDurationHeader(r *http.Request, headerName string, fallback time.Duration) time.Duration {
	raw := strings.TrimSpace(r.Header.Get(headerName))
	if raw == "" {
		return fallback
	}
	ms, err := strconv.Atoi(raw)
	if err != nil || ms < 0 {
		return fallback
	}
	return time.Duration(ms) * time.Millisecond
}

func sleepWithContext(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}
