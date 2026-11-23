package model

import (
	"encoding/json"
	"time"
)

// TraceStatus 表示单次模型或代理请求最终的结束状态。
type TraceStatus string

const (
	StatusOK            TraceStatus = "ok"             // 请求正常完成
	StatusAborted       TraceStatus = "aborted"        // 客户端中途主动断开连接
	StatusError         TraceStatus = "error"          // 上游返回错误或流式过程中报错
	StatusChaosInjected TraceStatus = "chaos_injected" // 被混沌故障中间件拦截并注入异常
)

// ToolCall 保存大模型在流式响应中拼接完整的单次工具调用信息。
type ToolCall struct {
	Index     int    `json:"index"`
	ID        string `json:"id"`
	Type      string `json:"type"`
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

// TraceMetrics 记录单次调用的延迟、吞吐和词元消耗统计。
type TraceMetrics struct {
	TTFTReasoningMs int64   `json:"ttft_reasoning_ms"` // 从发起请求到收到首个思考字的时间（毫秒）
	TTFTAnswerMs    int64   `json:"ttft_answer_ms"`    // 从发起请求到收到首个正文回答字的时间（毫秒）
	TotalMs         int64   `json:"total_ms"`          // 整个请求从开始到结束的总耗时（毫秒）
	TPS             float64 `json:"tps"`               // 每秒生成的词元数
	InputTokens     int     `json:"input_tokens"`
	OutputTokens    int     `json:"output_tokens"`
	TokensEstimated bool    `json:"tokens_estimated"` // 为 true 时表示上游未返回 usage，由本地估算
}

// ChaosInfo 记录命中混沌故障注入时的规则名与故障类型。
type ChaosInfo struct {
	RuleName  string `json:"rule_name"`
	FaultType string `json:"fault_type"`
}

// TraceRecord 是落盘到 .jsonl.gz 文件中的单条完整调用轨迹。
type TraceRecord struct {
	TraceID       string          `json:"trace_id"`
	SpanID        string          `json:"span_id,omitempty"`
	ParentSpanID  string          `json:"parent_span_id,omitempty"`
	AgentName     string          `json:"agent_name,omitempty"`
	SessionID     string          `json:"session_id"`
	Timestamp     time.Time       `json:"timestamp"`
	Method        string          `json:"method"`
	Path          string          `json:"path"`
	StatusCode    int             `json:"status_code"`
	Status        TraceStatus     `json:"status"`
	RequestModel  string          `json:"gen_ai.request.model,omitempty"`
	ResponseModel string          `json:"gen_ai.response.model,omitempty"`
	RequestBody   json.RawMessage `json:"request_body,omitempty"`
	ReasoningText string          `json:"reasoning_content,omitempty"`
	ContentText   string          `json:"content,omitempty"`
	ToolCalls     []ToolCall      `json:"tool_calls,omitempty"`
	FinishReason  string          `json:"finish_reason,omitempty"`
	RawError      string          `json:"raw_error,omitempty"`
	Metrics       TraceMetrics    `json:"metrics"`
	Chaos         *ChaosInfo      `json:"chaos,omitempty"`
}
