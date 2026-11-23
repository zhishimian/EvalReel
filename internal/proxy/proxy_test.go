package proxy

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"evalreel/internal/chaos"
	"evalreel/internal/config"
	"evalreel/internal/mockllm"
	"evalreel/internal/model"
	"evalreel/internal/observability"
	"evalreel/internal/recorder"
	"evalreel/internal/sse"
	"evalreel/internal/store"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestProxy_StreamPassThroughAndRecord(t *testing.T) {
	mockCfg := mockllm.DefaultConfig()
	mockCfg.TTFT = 10 * time.Millisecond
	mockCfg.ChunkInterval = 5 * time.Millisecond
	mockSrv := httptest.NewServer(mockllm.NewServer(mockCfg).Handler())
	defer mockSrv.Close()

	tracePath := filepath.Join(t.TempDir(), "stream.jsonl.gz")
	rec, err := recorder.NewGzipJSONLRecorder(tracePath)
	if err != nil {
		t.Fatalf("NewGzipJSONLRecorder: %v", err)
	}

	p, err := New(Options{
		UpstreamURL:    mockSrv.URL,
		SessionID:      "test-sess",
		Recorder:       rec,
		EstimateTokens: sse.HeuristicEstimateTokens,
	})
	if err != nil {
		t.Fatalf("proxy.New: %v", err)
	}
	proxySrv := httptest.NewServer(p.Handler())

	payload := `{"model":"mock-reasoning-v1","stream":true,"stream_options":{"include_usage":true},"messages":[{"role":"user","content":"你好"}]}`
	resp, err := proxySrv.Client().Post(proxySrv.URL+"/v1/chat/completions", "application/json", strings.NewReader(payload))
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("read body failed: %v", err)
	}

	proxySrv.Client().CloseIdleConnections()
	proxySrv.Close()
	p.CloseIdleConnections()
	if err := rec.Close(); err != nil {
		t.Fatalf("rec.Close: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if !strings.Contains(string(body), "data: [DONE]") {
		t.Fatalf("expected [DONE] in stream response")
	}

	traces := readTraces(t, tracePath)
	if len(traces) != 1 {
		t.Fatalf("expected 1 trace, got %d", len(traces))
	}
	tr := traces[0]
	if tr.Status != model.StatusOK || tr.ReasoningText == "" || tr.ContentText == "" {
		t.Fatalf("unexpected trace record: %+v", tr)
	}
	if tr.Metrics.InputTokens != 20 || tr.Metrics.OutputTokens != 32 || tr.Metrics.TokensEstimated {
		t.Fatalf("unexpected usage metrics: %+v", tr.Metrics)
	}
}

func TestProxy_NonStreamAndModelsPassThrough(t *testing.T) {
	mockCfg := mockllm.DefaultConfig()
	mockCfg.TTFT = 5 * time.Millisecond
	mockSrv := httptest.NewServer(mockllm.NewServer(mockCfg).Handler())
	defer mockSrv.Close()

	tracePath := filepath.Join(t.TempDir(), "nonstream.jsonl.gz")
	rec, err := recorder.NewGzipJSONLRecorder(tracePath)
	if err != nil {
		t.Fatalf("NewGzipJSONLRecorder: %v", err)
	}

	p, err := New(Options{
		UpstreamURL:    mockSrv.URL,
		SessionID:      "test-sess",
		Recorder:       rec,
		EstimateTokens: sse.HeuristicEstimateTokens,
	})
	if err != nil {
		t.Fatalf("proxy.New: %v", err)
	}
	proxySrv := httptest.NewServer(p.Handler())

	// 1. GET /v1/models 应原样透传且不写轨迹
	mResp, err := proxySrv.Client().Get(proxySrv.URL + "/v1/models")
	if err != nil {
		t.Fatalf("GET /v1/models failed: %v", err)
	}
	_, _ = io.ReadAll(mResp.Body)
	mResp.Body.Close()
	if mResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 on /v1/models, got %d", mResp.StatusCode)
	}

	// 2. 非流式 POST /v1/chat/completions 应透传并记录 1 条轨迹
	cResp, err := proxySrv.Client().Post(proxySrv.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"mock-reasoning-v1","stream":false,"messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatalf("POST non-stream failed: %v", err)
	}
	_, _ = io.ReadAll(cResp.Body)
	cResp.Body.Close()

	proxySrv.Client().CloseIdleConnections()
	proxySrv.Close()
	p.CloseIdleConnections()
	if err := rec.Close(); err != nil {
		t.Fatalf("rec.Close: %v", err)
	}

	traces := readTraces(t, tracePath)
	if len(traces) != 1 {
		t.Fatalf("expected exactly 1 trace (excluding /v1/models), got %d", len(traces))
	}
	if traces[0].Status != model.StatusOK || traces[0].ContentText == "" {
		t.Fatalf("unexpected non-stream trace: %+v", traces[0])
	}
}

func TestProxy_MidStreamClientCancel(t *testing.T) {
	var upstreamCanceled atomic.Bool
	canceledCh := make(chan struct{}, 1)

	mockCfg := mockllm.Config{
		TTFT:          10 * time.Millisecond,
		ChunkInterval: 40 * time.Millisecond,
		ModelName:     "mock-reasoning-v1",
		OnCanceled: func() {
			if upstreamCanceled.CompareAndSwap(false, true) {
				close(canceledCh)
			}
		},
	}
	mockSrv := httptest.NewServer(mockllm.NewServer(mockCfg).Handler())
	defer mockSrv.Close()

	tracePath := filepath.Join(t.TempDir(), "cancel.jsonl.gz")
	rec, err := recorder.NewGzipJSONLRecorder(tracePath)
	if err != nil {
		t.Fatalf("NewGzipJSONLRecorder: %v", err)
	}

	p, err := New(Options{
		UpstreamURL:    mockSrv.URL,
		SessionID:      "cancel-sess",
		Recorder:       rec,
		EstimateTokens: sse.HeuristicEstimateTokens,
	})
	if err != nil {
		t.Fatalf("proxy.New: %v", err)
	}
	proxySrv := httptest.NewServer(p.Handler())

	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, proxySrv.URL+"/v1/chat/completions",
		strings.NewReader(`{"model":"mock-reasoning-v1","stream":true,"messages":[{"role":"user","content":"cancel test"}]}`))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := proxySrv.Client().Do(req)
	if err != nil {
		t.Fatalf("Do failed: %v", err)
	}

	// 读取前 3 个有效 SSE 数据块后立刻断开客户端
	scanner := bufio.NewScanner(resp.Body)
	chunks := 0
	for scanner.Scan() {
		if strings.HasPrefix(scanner.Text(), "data: ") {
			chunks++
			if chunks >= 3 {
				cancel()
				break
			}
		}
	}
	resp.Body.Close()

	select {
	case <-canceledCh:
	case <-time.After(150 * time.Millisecond):
		t.Fatalf("expected upstream mockllm to stop within 150ms after client cancel")
	}

	proxySrv.Client().CloseIdleConnections()
	proxySrv.Close()
	p.CloseIdleConnections()
	if err := rec.Close(); err != nil {
		t.Fatalf("rec.Close: %v", err)
	}

	traces := readTraces(t, tracePath)
	if len(traces) != 1 {
		t.Fatalf("expected 1 aborted trace, got %d", len(traces))
	}
	if traces[0].Status != model.StatusAborted {
		t.Fatalf("expected StatusAborted, got %s", traces[0].Status)
	}
	if traces[0].ReasoningText == "" {
		t.Fatalf("expected partial reasoning text preserved in aborted trace")
	}
}

func TestProxy_Chaos429AndStreamCut(t *testing.T) {
	mockCfg := mockllm.DefaultConfig()
	mockCfg.TTFT = 5 * time.Millisecond
	mockCfg.ChunkInterval = 5 * time.Millisecond
	mockSrv := httptest.NewServer(mockllm.NewServer(mockCfg).Handler())
	defer mockSrv.Close()

	tracePath := filepath.Join(t.TempDir(), "chaos.jsonl.gz")
	rec, err := recorder.NewGzipJSONLRecorder(tracePath)
	if err != nil {
		t.Fatalf("NewGzipJSONLRecorder: %v", err)
	}

	chaosEngine := chaos.NewEngine(config.ChaosConfig{
		Enabled: true,
		Rules: []config.ChaosRule{
			{
				Name:          "every_2_429",
				MatchPath:     "/v1/chat/completions",
				EveryN:        2,
				Action:        "status_429",
				RetryAfterSec: 4,
			},
			{
				Name:           "cut_after_3",
				MatchPath:      "/v1/chat/completions",
				EveryN:         1,
				Action:         "cut_stream",
				CutAfterChunks: 3,
			},
		},
	})

	p, err := New(Options{
		UpstreamURL:    mockSrv.URL,
		SessionID:      "chaos-sess",
		Recorder:       rec,
		Chaos:          chaosEngine,
		EstimateTokens: sse.HeuristicEstimateTokens,
	})
	if err != nil {
		t.Fatalf("proxy.New: %v", err)
	}
	proxySrv := httptest.NewServer(p.Handler())

	// 第 1 次请求：未命中 every_n=2 的 429，走到流中被 cut_after_chunks=3 切断
	resp1, err := proxySrv.Client().Post(proxySrv.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"mock-reasoning-v1","stream":true,"messages":[{"role":"user","content":"1"}]}`))
	if err != nil {
		t.Fatalf("req1 failed: %v", err)
	}
	b1, _ := io.ReadAll(resp1.Body)
	resp1.Body.Close()
	if strings.Contains(string(b1), "[DONE]") {
		t.Fatalf("expected stream cut before [DONE], got:\n%s", string(b1))
	}

	// 第 2 次请求：命中请求前 429，直接拦截
	resp2, err := proxySrv.Client().Post(proxySrv.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"mock-reasoning-v1","stream":true,"messages":[{"role":"user","content":"2"}]}`))
	if err != nil {
		t.Fatalf("req2 failed: %v", err)
	}
	_, _ = io.ReadAll(resp2.Body)
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusTooManyRequests || resp2.Header.Get("Retry-After") != "4" {
		t.Fatalf("expected 429 with Retry-After=4, got %d (%q)", resp2.StatusCode, resp2.Header.Get("Retry-After"))
	}

	proxySrv.Client().CloseIdleConnections()
	proxySrv.Close()
	p.CloseIdleConnections()
	if err := rec.Close(); err != nil {
		t.Fatalf("rec.Close: %v", err)
	}

	traces := readTraces(t, tracePath)
	if len(traces) != 2 {
		t.Fatalf("expected 2 chaos traces, got %d", len(traces))
	}
	if traces[0].Status != model.StatusChaosInjected || traces[1].Status != model.StatusChaosInjected {
		t.Fatalf("expected both traces StatusChaosInjected, got %s and %s", traces[0].Status, traces[1].Status)
	}
}

func TestProxy_CompatibleModeV1PathJoin(t *testing.T) {
	var gotPath string
	upstreamSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"object":"list","data":[]}`)
	}))
	defer upstreamSrv.Close()

	p, err := New(Options{
		UpstreamURL: upstreamSrv.URL + "/compatible-mode/v1",
	})
	if err != nil {
		t.Fatalf("proxy.New: %v", err)
	}
	proxySrv := httptest.NewServer(p.Handler())
	defer func() {
		proxySrv.Client().CloseIdleConnections()
		proxySrv.Close()
		p.CloseIdleConnections()
	}()

	resp, err := proxySrv.Client().Get(proxySrv.URL + "/v1/models")
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	_, _ = io.ReadAll(resp.Body)
	resp.Body.Close()

	if gotPath != "/compatible-mode/v1/models" {
		t.Fatalf("expected upstream path /compatible-mode/v1/models, got %q", gotPath)
	}
}

func TestProxy_TraceparentAndSpanStore(t *testing.T) {
	mockCfg := mockllm.DefaultConfig()
	mockCfg.TTFT = 5 * time.Millisecond
	mockSrv := httptest.NewServer(mockllm.NewServer(mockCfg).Handler())
	defer mockSrv.Close()

	tracePath := filepath.Join(t.TempDir(), "multi.jsonl.gz")
	rec, err := recorder.NewGzipJSONLRecorder(tracePath)
	if err != nil {
		t.Fatalf("NewGzipJSONLRecorder: %v", err)
	}
	st, err := store.NewSQLiteStore(filepath.Join(t.TempDir(), "spans.db"))
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	defer st.Close()

	p, err := New(Options{
		UpstreamURL:    mockSrv.URL,
		SessionID:      "multi-sess",
		Recorder:       rec,
		SpanStore:      st,
		EstimateTokens: sse.HeuristicEstimateTokens,
	})
	if err != nil {
		t.Fatalf("proxy.New: %v", err)
	}
	proxySrv := httptest.NewServer(p.Handler())

	traceID := observability.NewTraceID()
	rootSpanID := observability.NewSpanID()

	// 1. planner 携带根 traceparent 请求。
	req1, _ := http.NewRequest(http.MethodPost, proxySrv.URL+"/v1/chat/completions",
		strings.NewReader(`{"model":"mock-reasoning-v1","stream":false,"messages":[{"role":"user","content":"plan"}]}`))
	req1.Header.Set("Content-Type", "application/json")
	req1.Header.Set("traceparent", observability.FormatTraceparent(traceID, rootSpanID))
	req1.Header.Set("X-EvalReel-Agent-Name", "planner")
	req1.Header.Set("X-EvalReel-Run-ID", "run-multi-1")
	req1.Header.Set("X-EvalReel-Task-ID", "tk-multi-1")

	resp1, err := proxySrv.Client().Do(req1)
	if err != nil {
		t.Fatalf("req1 failed: %v", err)
	}
	plannerSpanID := resp1.Header.Get("X-EvalReel-Span-ID")
	_, _ = io.ReadAll(resp1.Body)
	resp1.Body.Close()
	if plannerSpanID == "" {
		t.Fatalf("expected non-empty X-EvalReel-Span-ID header")
	}

	// 2. coder 以 planner 返回的 span_id 为父节点发起子请求。
	req2, _ := http.NewRequest(http.MethodPost, proxySrv.URL+"/v1/chat/completions",
		strings.NewReader(`{"model":"mock-reasoning-v1","stream":false,"messages":[{"role":"user","content":"code"}]}`))
	req2.Header.Set("Content-Type", "application/json")
	req2.Header.Set("traceparent", observability.FormatTraceparent(traceID, plannerSpanID))
	req2.Header.Set("X-EvalReel-Agent-Name", "coder")
	req2.Header.Set("X-EvalReel-Run-ID", "run-multi-1")
	req2.Header.Set("X-EvalReel-Task-ID", "tk-multi-1")

	resp2, err := proxySrv.Client().Do(req2)
	if err != nil {
		t.Fatalf("req2 failed: %v", err)
	}
	coderSpanID := resp2.Header.Get("X-EvalReel-Span-ID")
	_, _ = io.ReadAll(resp2.Body)
	resp2.Body.Close()

	proxySrv.Client().CloseIdleConnections()
	proxySrv.Close()
	p.CloseIdleConnections()
	if err := rec.Close(); err != nil {
		t.Fatalf("rec.Close: %v", err)
	}

	traces := readTraces(t, tracePath)
	if len(traces) != 2 {
		t.Fatalf("expected 2 traces, got %d", len(traces))
	}
	if traces[0].TraceID != traceID || traces[0].SpanID != plannerSpanID || traces[0].ParentSpanID != rootSpanID || traces[0].AgentName != "planner" {
		t.Fatalf("unexpected planner trace fields: %+v", traces[0])
	}
	if traces[1].TraceID != traceID || traces[1].SpanID != coderSpanID || traces[1].ParentSpanID != plannerSpanID || traces[1].AgentName != "coder" {
		t.Fatalf("unexpected coder trace fields: %+v", traces[1])
	}

	spans, err := st.ListSpansByTraceID(context.Background(), traceID)
	if err != nil || len(spans) != 2 {
		t.Fatalf("ListSpansByTraceID: len=%d err=%v", len(spans), err)
	}
	if spans[0].AgentName != "planner" || spans[1].AgentName != "coder" || spans[1].ParentSpanID != spans[0].SpanID {
		t.Fatalf("unexpected persisted spans hierarchy: %+v", spans)
	}
}

func readTraces(t *testing.T, path string) []model.TraceRecord {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()

	gz, err := gzip.NewReader(f)
	if err != nil {
		t.Fatalf("gzip reader: %v", err)
	}
	defer gz.Close()

	var out []model.TraceRecord
	sc := bufio.NewScanner(gz)
	sc.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for sc.Scan() {
		var tr model.TraceRecord
		if err := json.Unmarshal(sc.Bytes(), &tr); err != nil {
			t.Fatalf("unmarshal trace: %v", err)
		}
		out = append(out, tr)
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scanner err: %v", err)
	}
	return out
}
