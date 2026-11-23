package sse

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"evalreel/internal/model"

	"github.com/pkoukk/tiktoken-go"
	"github.com/tmaxmax/go-sse"
)

// DefaultMaxEventSize 将单条 SSE 事件长度上限放宽到 4MB，避开 go-sse 默认 64KB 限制。
const DefaultMaxEventSize = 4 * 1024 * 1024

// Result 保存单次流式响应旁路解析后的完整文本、工具调用和指标统计。
type Result struct {
	ResponseModel string
	ReasoningText string
	ContentText   string
	ToolCalls     []model.ToolCall
	FinishReason  string
	RawError      string
	Status        model.TraceStatus
	Completed     bool // 为 true 表示已收到 [DONE] 或 finish_reason，流式主体已完整结束
	Metrics       model.TraceMetrics
}

// ParseOptions 配置单次流式解析的起始时间、输入文本和词元估算器。
type ParseOptions struct {
	StartTime      time.Time
	RequestText    string
	MaxEventSize   int
	EstimateTokens func(string) int
}

// HeuristicEstimateTokens 提供零网络依赖的轻量词元数估算，用作断网或兜底方案。
func HeuristicEstimateTokens(text string) int {
	if text == "" {
		return 0
	}
	n := (len([]rune(text)) + 1) / 2
	if n < 1 {
		return 1
	}
	return n
}

// DefaultEstimateTokens 返回默认词元估算结果。
func DefaultEstimateTokens(text string) int {
	return HeuristicEstimateTokens(text)
}

// NewTiktokenEstimator 尝试加载 cl100k_base 词表；加载失败时自动降级到本地估算。
func NewTiktokenEstimator() func(string) int {
	var once sync.Once
	var enc *tiktoken.Tiktoken

	return func(text string) int {
		if text == "" {
			return 0
		}
		once.Do(func() {
			e, err := tiktoken.GetEncoding(tiktoken.MODEL_CL100K_BASE)
			http.DefaultClient.CloseIdleConnections()
			if err == nil {
				enc = e
			}
		})
		if enc != nil {
			return len(enc.EncodeOrdinary(text))
		}
		return HeuristicEstimateTokens(text)
	}
}

type sseChunk struct {
	Model string `json:"model"`
	Error *struct {
		Message string `json:"message"`
		Type    string `json:"type"`
	} `json:"error"`
	Usage *struct {
		PromptTokens     int `json:"prompt_tokens"`
		CompletionTokens int `json:"completion_tokens"`
		TotalTokens      int `json:"total_tokens"`
	} `json:"usage"`
	Choices []struct {
		Delta struct {
			ReasoningContent string `json:"reasoning_content"`
			Content          string `json:"content"`
			ToolCalls        []struct {
				Index    int    `json:"index"`
				ID       string `json:"id"`
				Type     string `json:"type"`
				Function struct {
					Name      string `json:"name"`
					Arguments string `json:"arguments"`
				} `json:"function"`
			} `json:"tool_calls"`
		} `json:"delta"`
		FinishReason *string `json:"finish_reason"`
	} `json:"choices"`
}

// ParseStream 从流式响应体中逐块解析思考内容、正文、工具调用与性能指标。
func ParseStream(r io.Reader, opts ParseOptions) Result {
	if opts.StartTime.IsZero() {
		opts.StartTime = time.Now()
	}
	if opts.MaxEventSize <= 0 {
		opts.MaxEventSize = DefaultMaxEventSize
	}
	if opts.EstimateTokens == nil {
		opts.EstimateTokens = DefaultEstimateTokens
	}

	var (
		firstReasoningAt time.Time
		firstAnswerAt    time.Time
		firstAnyTokenAt  time.Time
		reasoningSB      strings.Builder
		contentSB        strings.Builder
		toolMap          = make(map[int]*model.ToolCall)
		responseModel    string
		finishReason     string
		rawError         string
		status           = model.StatusOK
		hasUsage         bool
		sawDone          bool
		inputTokens      int
		outputTokens     int
	)

	cfg := &sse.ReadConfig{
		MaxEventSize: opts.MaxEventSize,
	}

	for ev, err := range sse.Read(r, cfg) {
		if err != nil {
			rawError = err.Error()
			status = model.StatusError
			break
		}

		data := strings.TrimSpace(ev.Data)
		if data == "" {
			continue
		}
		if data == "[DONE]" {
			sawDone = true
			break
		}

		var chunk sseChunk
		if umErr := json.Unmarshal([]byte(data), &chunk); umErr != nil {
			rawError = fmt.Sprintf("invalid json chunk: %v", umErr)
			status = model.StatusError
			break
		}

		if chunk.Error != nil {
			if chunk.Error.Message != "" {
				rawError = chunk.Error.Message
			} else {
				rawError = data
			}
			status = model.StatusError
			break
		}

		if chunk.Model != "" && responseModel == "" {
			responseModel = chunk.Model
		}

		if chunk.Usage != nil {
			hasUsage = true
			inputTokens = chunk.Usage.PromptTokens
			outputTokens = chunk.Usage.CompletionTokens
			if outputTokens == 0 && chunk.Usage.TotalTokens > chunk.Usage.PromptTokens {
				outputTokens = chunk.Usage.TotalTokens - chunk.Usage.PromptTokens
			}
		}

		if len(chunk.Choices) == 0 {
			continue
		}

		choice := chunk.Choices[0]
		now := time.Now()

		if choice.Delta.ReasoningContent != "" {
			if firstReasoningAt.IsZero() {
				firstReasoningAt = now
			}
			if firstAnyTokenAt.IsZero() {
				firstAnyTokenAt = now
			}
			reasoningSB.WriteString(choice.Delta.ReasoningContent)
		}

		if choice.Delta.Content != "" {
			if firstAnswerAt.IsZero() {
				firstAnswerAt = now
			}
			if firstAnyTokenAt.IsZero() {
				firstAnyTokenAt = now
			}
			contentSB.WriteString(choice.Delta.Content)
		}

		for _, tc := range choice.Delta.ToolCalls {
			if firstAnyTokenAt.IsZero() {
				firstAnyTokenAt = now
			}
			entry, exists := toolMap[tc.Index]
			if !exists {
				entry = &model.ToolCall{Index: tc.Index}
				toolMap[tc.Index] = entry
			}
			if tc.ID != "" {
				entry.ID = tc.ID
			}
			if tc.Type != "" {
				entry.Type = tc.Type
			}
			if tc.Function.Name != "" {
				entry.Name += tc.Function.Name
			}
			if tc.Function.Arguments != "" {
				entry.Arguments += tc.Function.Arguments
			}
		}

		if choice.FinishReason != nil && *choice.FinishReason != "" {
			finishReason = *choice.FinishReason
		}
	}

	toolCalls := assembleToolCalls(toolMap)
	endTime := time.Now()
	totalDur := endTime.Sub(opts.StartTime)
	if totalDur < 0 {
		totalDur = 0
	}

	var ttftReasoningMs int64
	if !firstReasoningAt.IsZero() {
		ttftReasoningMs = maxZeroMs(firstReasoningAt.Sub(opts.StartTime))
	}

	var ttftAnswerMs int64
	if !firstAnswerAt.IsZero() {
		ttftAnswerMs = maxZeroMs(firstAnswerAt.Sub(opts.StartTime))
	}

	tokensEstimated := false
	if !hasUsage {
		tokensEstimated = true
		inputTokens = opts.EstimateTokens(opts.RequestText)
		outputTokens = opts.EstimateTokens(combineOutputText(reasoningSB.String(), contentSB.String(), toolCalls))
	}

	var tps float64
	if outputTokens > 0 {
		genDur := totalDur
		if !firstAnyTokenAt.IsZero() {
			genDur = endTime.Sub(firstAnyTokenAt)
		}
		if genDur > 0 {
			tps = float64(outputTokens) / genDur.Seconds()
		}
	}

	return Result{
		ResponseModel: responseModel,
		ReasoningText: reasoningSB.String(),
		ContentText:   contentSB.String(),
		ToolCalls:     toolCalls,
		FinishReason:  finishReason,
		RawError:      rawError,
		Status:        status,
		Completed:     sawDone || finishReason != "",
		Metrics: model.TraceMetrics{
			TTFTReasoningMs: ttftReasoningMs,
			TTFTAnswerMs:    ttftAnswerMs,
			TotalMs:         totalDur.Milliseconds(),
			TPS:             tps,
			InputTokens:     inputTokens,
			OutputTokens:    outputTokens,
			TokensEstimated: tokensEstimated,
		},
	}
}

func assembleToolCalls(toolMap map[int]*model.ToolCall) []model.ToolCall {
	if len(toolMap) == 0 {
		return nil
	}
	indices := make([]int, 0, len(toolMap))
	for idx := range toolMap {
		indices = append(indices, idx)
	}
	sort.Ints(indices)

	calls := make([]model.ToolCall, 0, len(indices))
	for _, idx := range indices {
		calls = append(calls, *toolMap[idx])
	}
	return calls
}

func combineOutputText(reasoning, content string, calls []model.ToolCall) string {
	var sb strings.Builder
	sb.WriteString(reasoning)
	sb.WriteString(content)
	for _, tc := range calls {
		sb.WriteString(tc.Name)
		sb.WriteString(tc.Arguments)
	}
	return sb.String()
}

func maxZeroMs(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	return d.Milliseconds()
}
