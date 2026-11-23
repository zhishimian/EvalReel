package mcpproxy

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"time"

	"evalreel/internal/observability"
	"evalreel/internal/store"
)

// SpanWriter 定义将 MCP 工具调用节点写入数据库的最小接口，不强耦合特定存储实现。
type SpanWriter interface {
	SaveSpans(ctx context.Context, spans []store.TraceSpan) error
}

// Options 配置 MCP 拦截器运行模式、磁带录制、Mock 回放、混沌引擎与 Span 持久化。
type Options struct {
	Mode         string // record | mock | chaos
	MockFallback string // strict | passthrough
	Recorder     *TapeRecorder
	MockTape     *TapeStore
	Chaos        *ChaosEngine
	SpanStore    SpanWriter
	RunID        string
	TaskID       string
	Logger       *slog.Logger
}

// Interceptor 统一处理 MCP tools/call 报文的录制、只读 Mock、工具侧故障注入和调用树落库。
type Interceptor struct {
	opts   Options
	logger *slog.Logger
}

// NewInterceptor 创建并规范化默认配置的工具调用拦截器。
func NewInterceptor(opts Options) *Interceptor {
	if opts.Mode == "" {
		opts.Mode = "record"
	}
	if opts.MockFallback == "" {
		opts.MockFallback = "strict"
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Interceptor{opts: opts, logger: logger}
}

// HandleToolCall 拦截并处理一条 JSON-RPC tools/call 请求，返回最终要写回客户端的原始 JSON 字节流。
func (in *Interceptor) HandleToolCall(
	ctx context.Context,
	msg JSONRPCMessage,
	rawReq []byte,
	forward func(context.Context, []byte) ([]byte, error),
) ([]byte, string, error) {
	params, ok := ParseToolCall(msg)
	if !ok {
		resp, err := forward(ctx, rawReq)
		return resp, "", err
	}

	traceID := ""
	parentSpanID := ""
	if tc, hasParent := observability.ParseTraceparent(traceparentFromContext(ctx)); hasParent {
		traceID = tc.TraceID
		parentSpanID = tc.SpanID
	} else {
		traceID = observability.NewTraceID()
	}
	spanID := observability.NewSpanID()

	start := time.Now()
	canonArgs := CanonicalJSON(params.Arguments)
	argsHash := ToolCallKey(params.Name, params.Arguments)

	rec := MCPRecord{
		Timestamp:     start.UTC(),
		TraceID:       traceID,
		SpanID:        spanID,
		ParentSpanID:  parentSpanID,
		Method:        msg.Method,
		ToolName:      params.Name,
		ArgsCanonical: canonArgs,
		ArgsHash:      argsHash,
		RequestParams: msg.Params,
	}

	// 1. 优先判定工具侧混沌故障注入。
	if rule, hit := in.opts.Chaos.Evaluate(params.Name); hit {
		rec.ChaosRule = rule.Name
		switch rule.Action {
		case "delay":
			if !sleepWithContext(ctx, time.Duration(rule.DelayMs)*time.Millisecond) {
				return nil, spanID, ctx.Err()
			}
			// delay 规则等待完后继续透传真实调用或 Mock。
		case "timeout":
			wait := time.Duration(rule.DelayMs) * time.Millisecond
			if wait <= 0 {
				wait = 50 * time.Millisecond
			}
			_ = sleepWithContext(ctx, wait)
			errMsg := rule.ErrorMessage
			if errMsg == "" {
				errMsg = "mcp tool call timed out (chaos injected)"
			}
			rec.Error = &JSONRPCError{Code: -32001, Message: errMsg}
			rec.DurationMs = time.Since(start).Milliseconds()
			in.persist(rec, "chaos_injected", errMsg)
			out, err := BuildErrorResponse(msg.ID, -32001, errMsg)
			return out, spanID, err
		case "jsonrpc_error":
			code := rule.ErrorCode
			if code == 0 {
				code = -32000
			}
			errMsg := rule.ErrorMessage
			if errMsg == "" {
				errMsg = "mcp chaos jsonrpc error"
			}
			rec.Error = &JSONRPCError{Code: code, Message: errMsg}
			rec.DurationMs = time.Since(start).Milliseconds()
			in.persist(rec, "chaos_injected", errMsg)
			out, err := BuildErrorResponse(msg.ID, code, errMsg)
			return out, spanID, err
		case "tool_is_error":
			errMsg := rule.ErrorMessage
			if errMsg == "" {
				errMsg = fmt.Sprintf("tool %s execution failed (chaos injected)", params.Name)
			}
			rec.IsToolError = true
			rec.DurationMs = time.Since(start).Milliseconds()
			in.persist(rec, "chaos_injected", errMsg)
			out, err := BuildToolErrorResponse(msg.ID, errMsg)
			return out, spanID, err
		case "malformed_json":
			rec.DurationMs = time.Since(start).Milliseconds()
			in.persist(rec, "chaos_injected", "malformed_json")
			return []byte(`{"jsonrpc":"2.0","id":1,"result":{"broken":`), spanID, nil
		}
	}

	// 2. 离线只读 Mock 回放判定。
	if in.opts.Mode == "mock" {
		if matched, hit := in.opts.MockTape.Match(argsHash); hit {
			rec.ResponseResult = matched.ResponseResult
			rec.Error = matched.Error
			rec.IsToolError = matched.IsToolError
			rec.DurationMs = time.Since(start).Milliseconds()

			var (
				out []byte
				err error
			)
			if matched.Error != nil {
				out, err = BuildErrorResponse(msg.ID, matched.Error.Code, matched.Error.Message)
				in.persist(rec, "error", matched.Error.Message)
			} else {
				out, err = BuildResultResponse(msg.ID, matched.ResponseResult)
				st := "ok"
				if matched.IsToolError {
					st = "error"
				}
				in.persist(rec, st, "")
			}
			return out, spanID, err
		}
		if in.opts.MockFallback != "passthrough" {
			errMsg := fmt.Sprintf("mock tape miss for tool %q (hash=%s)", params.Name, argsHash[:12])
			rec.Error = &JSONRPCError{Code: -32004, Message: errMsg}
			rec.DurationMs = time.Since(start).Milliseconds()
			in.persist(rec, "error", errMsg)
			out, err := BuildErrorResponse(msg.ID, -32004, errMsg)
			return out, spanID, err
		}
	}

	// 3. 转发给真实上游 MCP 服务并解析响应记录磁带与调用树。
	if forward == nil {
		errMsg := "no upstream MCP server configured"
		out, err := BuildErrorResponse(msg.ID, -32004, errMsg)
		return out, spanID, err
	}
	rawResp, err := forward(ctx, rawReq)
	rec.DurationMs = time.Since(start).Milliseconds()
	if err != nil {
		rec.Error = &JSONRPCError{Code: -32603, Message: err.Error()}
		in.persist(rec, "error", err.Error())
		return nil, spanID, err
	}

	var respMsg JSONRPCMessage
	if uErr := json.Unmarshal(rawResp, &respMsg); uErr == nil {
		rec.ResponseResult = respMsg.Result
		rec.Error = respMsg.Error
		rec.IsToolError = detectToolIsError(respMsg.Result)
	}
	spanStatus := "ok"
	errText := ""
	if rec.ChaosRule != "" {
		spanStatus = "chaos_injected"
	} else if rec.Error != nil {
		spanStatus = "error"
		errText = rec.Error.Message
	} else if rec.IsToolError {
		spanStatus = "error"
		errText = "mcp tool returned isError=true"
	}
	in.persist(rec, spanStatus, errText)
	return rawResp, spanID, nil
}

func (in *Interceptor) persist(rec MCPRecord, spanStatus, errText string) {
	if in.opts.Recorder != nil {
		if err := in.opts.Recorder.Write(rec); err != nil {
			in.logger.Warn("write mcp tape record failed", "tool", rec.ToolName, "err", err)
		}
	}
	if in.opts.SpanStore != nil {
		saveCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		err := in.opts.SpanStore.SaveSpans(saveCtx, []store.TraceSpan{{
			SpanID:       rec.SpanID,
			ParentSpanID: rec.ParentSpanID,
			TraceID:      rec.TraceID,
			RunID:        in.opts.RunID,
			TaskID:       in.opts.TaskID,
			AgentName:    "mcp:" + rec.ToolName,
			Operation:    "mcp_tool",
			Model:        rec.ChaosRule,
			StartTimeMs:  rec.Timestamp.UnixMilli(),
			DurationMs:   rec.DurationMs,
			Status:       spanStatus,
			ErrorMsg:     errText,
		}})
		cancel()
		if err != nil {
			in.logger.Warn("save mcp trace span failed", "span_id", rec.SpanID, "err", err)
		}
	}
}

func detectToolIsError(raw json.RawMessage) bool {
	if len(raw) == 0 {
		return false
	}
	var payload struct {
		IsError bool `json:"isError"`
	}
	if err := json.Unmarshal(raw, &payload); err == nil {
		return payload.IsError
	}
	return false
}

type traceparentCtxKey struct{}

// WithTraceparent 将 W3C traceparent 请求头或环境变量绑定到上下文中供 MCP 代理继承链路。
func WithTraceparent(ctx context.Context, tp string) context.Context {
	return context.WithValue(ctx, traceparentCtxKey{}, tp)
}

func traceparentFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(traceparentCtxKey{}).(string); ok {
		return v
	}
	return ""
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
