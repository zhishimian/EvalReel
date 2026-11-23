package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"evalreel/internal/chaos"
	"evalreel/internal/config"
	"evalreel/internal/model"
	"evalreel/internal/observability"
	"evalreel/internal/recorder"
	"evalreel/internal/sse"
	"evalreel/internal/store"

	"go.opentelemetry.io/otel/trace"
)

const defaultMaxBodyBytes = 10 * 1024 * 1024

type metaCtxKey struct{}

type requestMeta struct {
	traceID      string
	spanID       string
	parentSpanID string
	agentName    string
	runID        string
	taskID       string
	startTime    time.Time
	method       string
	path         string
	requestModel string
	requestBody  json.RawMessage
	requestText  string
	isChatPath   bool
	chaosInfo    *model.ChaosInfo
	otelSpan     trace.Span
	recorded     atomic.Bool
}

// SpanWriter 定义将代理捕获的父子调用节点持久化到数据库的能力。
type SpanWriter interface {
	SaveSpans(ctx context.Context, spans []store.TraceSpan) error
}

// Options 配置反向代理的上游目标、会话信息、落盘器、Span 存储与混沌引擎。
type Options struct {
	UpstreamURL      string
	UpstreamTimeout  time.Duration
	SessionID        string
	MaxRequestBodyMB int
	Recorder         recorder.Recorder
	SpanStore        SpanWriter
	Chaos            *chaos.Engine
	EstimateTokens   func(string) int
	Logger           *slog.Logger
}

// Proxy 封装了兼容 OpenAI 流式透传、旁路录制与混沌故障注入的反向代理。
type Proxy struct {
	opts           Options
	target         *url.URL
	transport      *http.Transport
	rp             *httputil.ReverseProxy
	maxBodyBytes   int64
	traceSeq       atomic.Uint64
	logger         *slog.Logger
	estimateTokens func(string) int
}

// New 创建并初始化透明流式反向代理实例。
func New(opts Options) (*Proxy, error) {
	target, err := url.Parse(opts.UpstreamURL)
	if err != nil || target.Scheme == "" || target.Host == "" {
		return nil, fmt.Errorf("invalid upstream url %q", opts.UpstreamURL)
	}

	maxBody := int64(opts.MaxRequestBodyMB) * 1024 * 1024
	if maxBody <= 0 {
		maxBody = defaultMaxBodyBytes
	}

	timeout := opts.UpstreamTimeout
	if timeout <= 0 {
		timeout = 120 * time.Second
	}

	logger := opts.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	est := opts.EstimateTokens
	if est == nil {
		est = sse.DefaultEstimateTokens
	}

	transport := &http.Transport{
		// 必须禁用标准库自带的透明 gzip 压缩，防止上游返回压缩流导致旁路解析乱码。
		DisableCompression:    true,
		MaxIdleConns:          200,
		MaxIdleConnsPerHost:   100,
		IdleConnTimeout:       8 * time.Second,
		ResponseHeaderTimeout: timeout,
	}

	p := &Proxy{
		opts:           opts,
		target:         target,
		transport:      transport,
		maxBodyBytes:   maxBody,
		logger:         logger,
		estimateTokens: est,
	}

	p.rp = &httputil.ReverseProxy{
		FlushInterval:  -1, // 收到上游一小块 SSE 数据立刻刷给客户端，不做缓冲攒批
		Transport:      transport,
		Rewrite:        p.rewriteRequest,
		ModifyResponse: p.modifyResponse,
		ErrorHandler:   p.errorHandler,
	}

	return p, nil
}

// Handler 返回按顺序串好请求体抓取、请求前混沌中间件和反向代理的 HTTP 处理器。
func (p *Proxy) Handler() http.Handler {
	core := http.Handler(p.rp)
	if p.opts.Chaos != nil {
		core = p.opts.Chaos.PreForward(
			core,
			func(r *http.Request, rule config.ChaosRule) *http.Request {
				if meta, ok := r.Context().Value(metaCtxKey{}).(*requestMeta); ok {
					meta.chaosInfo = &model.ChaosInfo{
						RuleName:  rule.Name,
						FaultType: rule.Action,
					}
				}
				return r
			},
			func(r *http.Request, rule config.ChaosRule, statusCode int) {
				meta, ok := r.Context().Value(metaCtxKey{}).(*requestMeta)
				if !ok || !meta.recorded.CompareAndSwap(false, true) {
					return
				}
				rec := model.TraceRecord{
					TraceID:      meta.traceID,
					SpanID:       meta.spanID,
					ParentSpanID: meta.parentSpanID,
					AgentName:    meta.agentName,
					SessionID:    p.opts.SessionID,
					Timestamp:    meta.startTime.UTC(),
					Method:       meta.method,
					Path:         meta.path,
					StatusCode:   statusCode,
					Status:       model.StatusChaosInjected,
					RequestModel: meta.requestModel,
					RequestBody:  meta.requestBody,
					RawError:     fmt.Sprintf("chaos rule %q triggered (%s)", rule.Name, rule.Action),
					Metrics: model.TraceMetrics{
						TotalMs: time.Since(meta.startTime).Milliseconds(),
					},
					Chaos: &model.ChaosInfo{
						RuleName:  rule.Name,
						FaultType: rule.Action,
					},
				}
				p.writeRecord(meta, rec)
			},
		)
	}
	return p.captureMiddleware(core)
}

// CloseIdleConnections 释放上游连接池中的空闲长连接，用于优雅退出与测试清理。
func (p *Proxy) CloseIdleConnections() {
	if p.transport != nil {
		p.transport.CloseIdleConnections()
	}
}

func (p *Proxy) captureMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		spanID := observability.NewSpanID()
		var traceID, parentSpanID string
		if tc, ok := observability.ParseTraceparent(r.Header.Get("traceparent")); ok {
			traceID = tc.TraceID
			parentSpanID = tc.SpanID
			ctx = observability.ContextWithParent(ctx, tc)
		} else {
			traceID = observability.NewTraceID()
		}

		agentName := strings.TrimSpace(r.Header.Get("X-EvalReel-Agent-Name"))
		runID := strings.TrimSpace(r.Header.Get("X-EvalReel-Run-ID"))
		taskID := strings.TrimSpace(r.Header.Get("X-EvalReel-Task-ID"))
		isChatPath := strings.HasSuffix(r.URL.Path, "/chat/completions")

		meta := &requestMeta{
			traceID:      traceID,
			spanID:       spanID,
			parentSpanID: parentSpanID,
			agentName:    agentName,
			runID:        runID,
			taskID:       taskID,
			startTime:    time.Now(),
			method:       r.Method,
			path:         r.URL.Path,
			isChatPath:   isChatPath,
		}

		if r.Body != nil && r.Method == http.MethodPost {
			bodyBytes, err := io.ReadAll(io.LimitReader(r.Body, p.maxBodyBytes))
			_ = r.Body.Close()
			if err == nil {
				r.Body = io.NopCloser(bytes.NewReader(bodyBytes))
				r.ContentLength = int64(len(bodyBytes))
				if json.Valid(bodyBytes) {
					meta.requestBody = json.RawMessage(bodyBytes)
				}
				meta.requestModel, meta.requestText = extractRequestFields(bodyBytes)
			}
		}

		if isChatPath {
			w.Header().Set("X-EvalReel-Span-ID", spanID)
			w.Header().Set("X-EvalReel-Trace-ID", traceID)
			var otelSpan trace.Span
			ctx, otelSpan = observability.StartGenAISpan(ctx, "evalreel.proxy.chat", observability.GenAIAttrs{
				System:       "openai",
				RequestModel: meta.requestModel,
				RunID:        runID,
				TaskID:       taskID,
				AgentName:    agentName,
			})
			meta.otelSpan = otelSpan
		}

		ctx = context.WithValue(ctx, metaCtxKey{}, meta)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (p *Proxy) rewriteRequest(pr *httputil.ProxyRequest) {
	pr.SetURL(p.target)

	// 若上游 base_url 带路径前缀（例如 /v1 或 /compatible-mode/v1），避免和客户端的 /v1/... 重复拼接。
	targetBase := strings.TrimRight(p.target.Path, "/")
	if targetBase != "" {
		switch {
		case pr.In.URL.Path == targetBase || strings.HasPrefix(pr.In.URL.Path, targetBase+"/"):
			pr.Out.URL.Path = pr.In.URL.Path
			pr.Out.URL.RawPath = pr.In.URL.RawPath
		case strings.HasSuffix(targetBase, "/v1") && (pr.In.URL.Path == "/v1" || strings.HasPrefix(pr.In.URL.Path, "/v1/")):
			pr.Out.URL.Path = targetBase + strings.TrimPrefix(pr.In.URL.Path, "/v1")
			pr.Out.URL.RawPath = ""
		}
	}

	pr.Out.Host = p.target.Host
	pr.Out.Header.Del("Accept-Encoding")
}

func (p *Proxy) modifyResponse(resp *http.Response) error {
	meta, ok := resp.Request.Context().Value(metaCtxKey{}).(*requestMeta)
	if !ok || !meta.isChatPath {
		return nil
	}

	ct := strings.ToLower(resp.Header.Get("Content-Type"))
	if strings.Contains(ct, "text/event-stream") {
		body := resp.Body
		var streamRule *config.ChaosRule
		if p.opts.Chaos != nil {
			streamRule = p.opts.Chaos.MatchStreamRule(meta.path)
			if streamRule != nil {
				body = chaos.WrapStream(body, streamRule)
				meta.chaosInfo = &model.ChaosInfo{
					RuleName:  streamRule.Name,
					FaultType: streamRule.Action,
				}
			}
		}

		opts := sse.ParseOptions{
			StartTime:      meta.startTime,
			RequestText:    meta.requestText,
			EstimateTokens: p.estimateTokens,
		}

		observability.DefaultMetrics().IncActiveStreams()
		resp.Body = sse.NewTapReadCloser(resp.Request.Context(), body, opts, func(res sse.Result) {
			observability.DefaultMetrics().DecActiveStreams()
			if !meta.recorded.CompareAndSwap(false, true) {
				return
			}
			status := res.Status
			if streamRule != nil && status != model.StatusAborted && (status == model.StatusError || streamRule.Action == "malformed_json") {
				status = model.StatusChaosInjected
			}
			rec := model.TraceRecord{
				TraceID:       meta.traceID,
				SpanID:        meta.spanID,
				ParentSpanID:  meta.parentSpanID,
				AgentName:     meta.agentName,
				SessionID:     p.opts.SessionID,
				Timestamp:     meta.startTime.UTC(),
				Method:        meta.method,
				Path:          meta.path,
				StatusCode:    resp.StatusCode,
				Status:        status,
				RequestModel:  meta.requestModel,
				ResponseModel: res.ResponseModel,
				RequestBody:   meta.requestBody,
				ReasoningText: res.ReasoningText,
				ContentText:   res.ContentText,
				ToolCalls:     res.ToolCalls,
				FinishReason:  res.FinishReason,
				RawError:      res.RawError,
				Metrics:       res.Metrics,
				Chaos:         meta.chaosInfo,
			}
			p.writeRecord(meta, rec)
		})
		return nil
	}

	// 非流式 JSON 或上游 4xx/5xx 报错：读完还原 Body 并落盘。
	respBytes, err := io.ReadAll(io.LimitReader(resp.Body, p.maxBodyBytes))
	_ = resp.Body.Close()
	if err != nil {
		return err
	}
	resp.Body = io.NopCloser(bytes.NewReader(respBytes))
	resp.ContentLength = int64(len(respBytes))
	resp.Header.Set("Content-Length", strconv.Itoa(len(respBytes)))

	if meta.recorded.CompareAndSwap(false, true) {
		rec := p.buildNonStreamRecord(meta, resp.StatusCode, respBytes)
		p.writeRecord(meta, rec)
	}
	return nil
}

func (p *Proxy) errorHandler(w http.ResponseWriter, r *http.Request, err error) {
	canceled := errors.Is(err, context.Canceled) || r.Context().Err() != nil

	if meta, ok := r.Context().Value(metaCtxKey{}).(*requestMeta); ok {
		if meta.recorded.CompareAndSwap(false, true) {
			status := model.StatusError
			code := http.StatusBadGateway
			if canceled {
				status = model.StatusAborted
				code = 499
			}
			p.writeRecord(meta, model.TraceRecord{
				TraceID:      meta.traceID,
				SpanID:       meta.spanID,
				ParentSpanID: meta.parentSpanID,
				AgentName:    meta.agentName,
				SessionID:    p.opts.SessionID,
				Timestamp:    meta.startTime.UTC(),
				Method:       meta.method,
				Path:         meta.path,
				StatusCode:   code,
				Status:       status,
				RequestModel: meta.requestModel,
				RequestBody:  meta.requestBody,
				RawError:     err.Error(),
				Metrics: model.TraceMetrics{
					TotalMs: time.Since(meta.startTime).Milliseconds(),
				},
				Chaos: meta.chaosInfo,
			})
		}
	}

	if canceled {
		p.logger.Info("client canceled request", "path", r.URL.Path)
		return
	}

	p.logger.Error("upstream proxy error", "path", r.URL.Path, "err", err)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusBadGateway)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{
			"message": err.Error(),
			"type":    "proxy_error",
		},
	})
}

func (p *Proxy) buildNonStreamRecord(meta *requestMeta, statusCode int, respBytes []byte) model.TraceRecord {
	totalMs := time.Since(meta.startTime).Milliseconds()
	rec := model.TraceRecord{
		TraceID:      meta.traceID,
		SpanID:       meta.spanID,
		ParentSpanID: meta.parentSpanID,
		AgentName:    meta.agentName,
		SessionID:    p.opts.SessionID,
		Timestamp:    meta.startTime.UTC(),
		Method:       meta.method,
		Path:         meta.path,
		StatusCode:   statusCode,
		Status:       model.StatusOK,
		RequestModel: meta.requestModel,
		RequestBody:  meta.requestBody,
		Chaos:        meta.chaosInfo,
		Metrics: model.TraceMetrics{
			TotalMs: totalMs,
		},
	}

	var parsed struct {
		Model string `json:"model"`
		Error *struct {
			Message string `json:"message"`
		} `json:"error"`
		Choices []struct {
			Message struct {
				ReasoningContent string           `json:"reasoning_content"`
				Content          string           `json:"content"`
				ToolCalls        []model.ToolCall `json:"tool_calls"`
			} `json:"message"`
			FinishReason string `json:"finish_reason"`
		} `json:"choices"`
		Usage *struct {
			PromptTokens     int `json:"prompt_tokens"`
			CompletionTokens int `json:"completion_tokens"`
		} `json:"usage"`
	}

	if err := json.Unmarshal(respBytes, &parsed); err != nil {
		rec.Status = model.StatusError
		rec.RawError = fmt.Sprintf("invalid response json: %v", err)
		return rec
	}

	rec.ResponseModel = parsed.Model
	if parsed.Error != nil || statusCode >= 400 {
		rec.Status = model.StatusError
		if parsed.Error != nil && parsed.Error.Message != "" {
			rec.RawError = parsed.Error.Message
		} else {
			rec.RawError = string(respBytes)
		}
	}

	if len(parsed.Choices) > 0 {
		msg := parsed.Choices[0].Message
		rec.ReasoningText = msg.ReasoningContent
		rec.ContentText = msg.Content
		rec.ToolCalls = msg.ToolCalls
		rec.FinishReason = parsed.Choices[0].FinishReason
	}

	if parsed.Usage != nil {
		rec.Metrics.InputTokens = parsed.Usage.PromptTokens
		rec.Metrics.OutputTokens = parsed.Usage.CompletionTokens
	} else {
		rec.Metrics.TokensEstimated = true
		rec.Metrics.InputTokens = p.estimateTokens(meta.requestText)
		rec.Metrics.OutputTokens = p.estimateTokens(rec.ReasoningText + rec.ContentText)
	}

	if rec.Metrics.OutputTokens > 0 && totalMs > 0 {
		rec.Metrics.TPS = float64(rec.Metrics.OutputTokens) / (float64(totalMs) / 1000.0)
	}
	return rec
}

func (p *Proxy) writeRecord(meta *requestMeta, rec model.TraceRecord) {
	m := observability.DefaultMetrics()
	chaosRule := ""
	if rec.Chaos != nil {
		chaosRule = rec.Chaos.RuleName
	}
	m.RecordProxyRequest(rec.RequestModel, string(rec.Status), chaosRule)
	m.ObserveTTFT("proxy", rec.RequestModel, "reasoning", rec.Metrics.TTFTReasoningMs)
	m.ObserveTTFT("proxy", rec.RequestModel, "answer", rec.Metrics.TTFTAnswerMs)

	if meta != nil && meta.otelSpan != nil {
		var spanErr error
		if rec.RawError != "" {
			spanErr = errors.New(rec.RawError)
		}
		observability.EndGenAISpan(meta.otelSpan, observability.GenAIResult{
			ResponseModel: rec.ResponseModel,
			InputTokens:   rec.Metrics.InputTokens,
			OutputTokens:  rec.Metrics.OutputTokens,
			FinishReason:  rec.FinishReason,
			Status:        string(rec.Status),
			Err:           spanErr,
		})
	}

	if p.opts.SpanStore != nil && meta != nil && meta.spanID != "" {
		ttftMs := rec.Metrics.TTFTAnswerMs
		if ttftMs == 0 {
			ttftMs = rec.Metrics.TTFTReasoningMs
		}
		modelName := rec.ResponseModel
		if modelName == "" {
			modelName = rec.RequestModel
		}
		saveCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		err := p.opts.SpanStore.SaveSpans(saveCtx, []store.TraceSpan{{
			SpanID:       meta.spanID,
			ParentSpanID: meta.parentSpanID,
			TraceID:      rec.TraceID,
			RunID:        meta.runID,
			TaskID:       meta.taskID,
			AgentName:    meta.agentName,
			Operation:    "llm_chat",
			Model:        modelName,
			StartTimeMs:  meta.startTime.UnixMilli(),
			DurationMs:   rec.Metrics.TotalMs,
			TTFTMs:       ttftMs,
			InputTokens:  rec.Metrics.InputTokens,
			OutputTokens: rec.Metrics.OutputTokens,
			Status:       string(rec.Status),
			ErrorMsg:     rec.RawError,
		}})
		cancel()
		if err != nil {
			p.logger.Warn("save trace span failed", "trace_id", rec.TraceID, "span_id", meta.spanID, "err", err)
		}
	}

	if p.opts.Recorder == nil {
		return
	}
	if err := p.opts.Recorder.Write(rec); err != nil {
		p.logger.Warn("write trace record failed", "trace_id", rec.TraceID, "err", err)
	}
}

func extractRequestFields(bodyBytes []byte) (string, string) {
	var req struct {
		Model    string `json:"model"`
		Messages []struct {
			Content any `json:"content"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(bodyBytes, &req); err != nil {
		return "", ""
	}
	var sb strings.Builder
	for _, m := range req.Messages {
		if s, ok := m.Content.(string); ok {
			sb.WriteString(s)
			sb.WriteByte('\n')
		}
	}
	return req.Model, sb.String()
}
