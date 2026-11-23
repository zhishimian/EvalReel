package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/trace"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestLogger_TextAndJSON(t *testing.T) {
	var textBuf bytes.Buffer
	tl := NewLogger(LogConfig{Service: "evalreel-test", Format: "text", Level: "warn"}, &textBuf)
	tl.Info("should-skip")
	tl.Warn("warn-msg", "run_id", "r1")
	if strings.Contains(textBuf.String(), "should-skip") || !strings.Contains(textBuf.String(), "warn-msg") {
		t.Fatalf("unexpected text logger output: %q", textBuf.String())
	}

	var jsonBuf bytes.Buffer
	jl := NewLogger(LogConfig{Service: "evalreel-worker", Format: "json", Level: "debug"}, &jsonBuf)
	jl.Debug("task-done", "task_id", "tk-1")

	var row map[string]any
	if err := json.Unmarshal(jsonBuf.Bytes(), &row); err != nil {
		t.Fatalf("expected valid json log line: %v (raw=%q)", err, jsonBuf.String())
	}
	if row["service"] != "evalreel-worker" || row["msg"] != "task-done" || row["task_id"] != "tk-1" {
		t.Fatalf("unexpected json fields: %+v", row)
	}
}

func TestMetrics_Exposition(t *testing.T) {
	m := NewMetrics()
	m.RecordProxyRequest("qwen3.8-flash", "ok", "")
	m.IncActiveStreams()
	m.DecActiveStreams()
	m.ObserveTTFT("proxy", "qwen3.8-flash", "answer", 180)
	m.RecordWorkerTask("qwen3.8-flash", "success", 420)
	m.ObserveLimiterWait("qwen3.8-flash", 35)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	m.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	body, _ := io.ReadAll(rec.Body)
	text := string(body)
	for _, want := range []string{
		"evalreel_proxy_requests_total",
		"evalreel_proxy_active_streams",
		"evalreel_llm_ttft_ms",
		"evalreel_worker_tasks_total",
		"evalreel_worker_task_duration_ms",
		"evalreel_limiter_wait_ms",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("metrics output missing %q", want)
		}
	}
}

func TestTraceparent_ParseFormatAndInherit(t *testing.T) {
	tid := NewTraceID()
	sid := NewSpanID()
	hdr := FormatTraceparent(tid, sid)

	tc, ok := ParseTraceparent(hdr)
	if !ok || tc.TraceID != tid || tc.SpanID != sid || tc.Flags != "01" {
		t.Fatalf("ParseTraceparent mismatch: ok=%v tc=%+v want=(%s,%s)", ok, tc, tid, sid)
	}

	invalids := []string{
		"",
		"01-" + tid + "-" + sid + "-01",
		"00-00000000000000000000000000000000-" + sid + "-01",
		"00-" + tid + "-0000000000000000-01",
		"00-nothex-" + sid + "-01",
	}
	for _, bad := range invalids {
		if _, ok := ParseTraceparent(bad); ok {
			t.Fatalf("expected invalid traceparent %q to fail", bad)
		}
	}

	parentCtx := ContextWithParent(context.Background(), tc)
	sc := trace.SpanContextFromContext(parentCtx)
	if !sc.IsValid() || sc.TraceID().String() != tid || sc.SpanID().String() != sid {
		t.Fatalf("unexpected injected SpanContext: %+v", sc)
	}
}

func TestOTel_StdoutSpanAndNoop(t *testing.T) {
	ctx := context.Background()

	// 1. none 模式可安全关闭且不产生协程泄漏。
	noopShutdown, err := InitTracer(ctx, OTelConfig{ServiceName: "test-noop", Exporter: "none"})
	if err != nil {
		t.Fatalf("InitTracer none: %v", err)
	}
	if err := noopShutdown(ctx); err != nil {
		t.Fatalf("noopShutdown: %v", err)
	}

	// 2. stdout 模式验证 gen_ai.* 属性和父子 TraceID 继承。
	var out bytes.Buffer
	shutdown, err := InitTracer(ctx, OTelConfig{
		ServiceName: "evalreel-test",
		Exporter:    "stdout",
		StdoutOut:   &out,
	})
	if err != nil {
		t.Fatalf("InitTracer stdout: %v", err)
	}
	defer func() {
		if err := shutdown(context.Background()); err != nil {
			t.Errorf("otel shutdown: %v", err)
		}
	}()

	parentTC := TraceContext{TraceID: NewTraceID(), SpanID: NewSpanID(), Flags: "01"}
	parentCtx := ContextWithParent(ctx, parentTC)

	_, span := StartGenAISpan(parentCtx, "evalreel.test.chat", GenAIAttrs{
		System:       "openai",
		RequestModel: "qwen3.8-flash",
		RunID:        "run-1",
		TaskID:       "tk-1",
		AgentName:    "planner",
	})
	EndGenAISpan(span, GenAIResult{
		ResponseModel: "qwen3.8-flash",
		InputTokens:   15,
		OutputTokens:  25,
		FinishReason:  "stop",
		Status:        "error",
		Err:           errors.New("mock err"),
	})

	exported := out.String()
	for _, want := range []string{
		parentTC.TraceID,
		parentTC.SpanID,
		"gen_ai.request.model",
		"gen_ai.usage.input_tokens",
		"evalreel.agent_name",
		"planner",
	} {
		if !strings.Contains(exported, want) {
			t.Fatalf("stdout otel span missing %q in:\n%s", want, exported)
		}
	}
}
