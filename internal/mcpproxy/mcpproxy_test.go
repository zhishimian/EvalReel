package mcpproxy

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"evalreel/internal/observability"
	"evalreel/internal/store"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestCanonicalJSONAndTapeStore(t *testing.T) {
	k1 := ToolCallKey("get_weather", json.RawMessage(`{"city":"Beijing","unit":"celsius"}`))
	k2 := ToolCallKey("get_weather", json.RawMessage(`{ "unit" : "celsius", "city" : "Beijing" }`))
	if k1 != k2 {
		t.Fatalf("expected identical key regardless of field order: %s vs %s", k1, k2)
	}

	tapePath := filepath.Join(t.TempDir(), "tools.mcp.jsonl.gz")
	rec, err := NewTapeRecorder(tapePath)
	if err != nil {
		t.Fatalf("NewTapeRecorder: %v", err)
	}
	_ = rec.Write(MCPRecord{
		Method:         "tools/call",
		ToolName:       "get_weather",
		ArgsCanonical:  CanonicalJSON(json.RawMessage(`{"city":"Beijing"}`)),
		ArgsHash:       ToolCallKey("get_weather", json.RawMessage(`{"city":"Beijing"}`)),
		ResponseResult: json.RawMessage(`{"content":[{"type":"text","text":"sunny 22C"}]}`),
	})
	_ = rec.Close()

	ts, err := LoadTape(tapePath)
	if err != nil {
		t.Fatalf("LoadTape: %v", err)
	}
	hit, ok := ts.Match(ToolCallKey("get_weather", json.RawMessage(`{"city":"Beijing"}`)))
	if !ok || !strings.Contains(string(hit.ResponseResult), "sunny 22C") {
		t.Fatalf("expected tape match, got ok=%v rec=%+v", ok, hit)
	}
}

func TestHTTPProxy_RecordMockAndChaos(t *testing.T) {
	var upstreamHits atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamHits.Add(1)
		body, _ := io.ReadAll(r.Body)
		var msg JSONRPCMessage
		_ = json.Unmarshal(body, &msg)

		// 非 tools/call 或 Streamable HTTP SSE 测试路径
		if msg.Method == "tools/list" {
			w.Header().Set("Content-Type", "application/json")
			out, _ := BuildResultResponse(msg.ID, json.RawMessage(`{"tools":[{"name":"get_weather"}]}`))
			_, _ = w.Write(out)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		res, _ := BuildResultResponse(msg.ID, json.RawMessage(`{"content":[{"type":"text","text":"clear 25C"}]}`))
		_, _ = w.Write([]byte("event: message\ndata: " + string(res) + "\n\n"))
	}))
	defer upstream.Close()

	tapePath := filepath.Join(t.TempDir(), "http.mcp.jsonl.gz")
	rec, err := NewTapeRecorder(tapePath)
	if err != nil {
		t.Fatalf("NewTapeRecorder: %v", err)
	}
	st, err := store.NewSQLiteStore(filepath.Join(t.TempDir(), "mcp_spans.db"))
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	defer st.Close()

	recProxy := NewHTTPProxy(NewInterceptor(Options{
		Mode:      "record",
		Recorder:  rec,
		SpanStore: st,
		RunID:     "run-mcp-1",
		TaskID:    "tk-mcp-1",
	}), upstream.URL, 5*time.Second)
	recSrv := httptest.NewServer(recProxy)

	traceID := observability.NewTraceID()
	rootSpan := observability.NewSpanID()

	// 1. 转发 tools/list + SSE tools/call 录制磁带与调用树 Span。
	rList, err := recSrv.Client().Post(recSrv.URL, "application/json", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`))
	if err != nil || rList.StatusCode != 200 {
		t.Fatalf("tools/list failed: err=%v resp=%+v", err, rList)
	}
	rList.Body.Close()

	reqCall, _ := http.NewRequest(http.MethodPost, recSrv.URL, strings.NewReader(`{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"get_weather","arguments":{"city":"Shanghai","unit":"c"}}}`))
	reqCall.Header.Set("Content-Type", "application/json")
	reqCall.Header.Set("traceparent", observability.FormatTraceparent(traceID, rootSpan))
	rCall, err := recSrv.Client().Do(reqCall)
	if err != nil {
		t.Fatalf("tools/call failed: %v", err)
	}
	callBody, _ := io.ReadAll(rCall.Body)
	rCall.Body.Close()
	if !strings.Contains(string(callBody), "clear 25C") || rCall.Header.Get("X-EvalReel-Span-ID") == "" {
		t.Fatalf("unexpected tools/call response: %s", string(callBody))
	}

	recSrv.Client().CloseIdleConnections()
	recSrv.Close()
	recProxy.CloseIdleConnections()
	_ = rec.Close()

	spans, err := st.ListSpansByTraceID(context.Background(), traceID)
	if err != nil || len(spans) != 1 || spans[0].AgentName != "mcp:get_weather" || spans[0].ParentSpanID != rootSpan {
		t.Fatalf("unexpected mcp span: spans=%+v err=%v", spans, err)
	}

	// 2. 严格离线 Mock 模式（不经过上游，打乱参数键序仍命中）。
	hitsBeforeMock := upstreamHits.Load()
	tape, err := LoadTape(tapePath)
	if err != nil {
		t.Fatalf("LoadTape: %v", err)
	}
	mockProxy := NewHTTPProxy(NewInterceptor(Options{
		Mode:         "mock",
		MockFallback: "strict",
		MockTape:     tape,
	}), "", 5*time.Second)
	mockSrv := httptest.NewServer(mockProxy)

	rMockHit, err := mockSrv.Client().Post(mockSrv.URL, "application/json",
		strings.NewReader(`{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"get_weather","arguments":{"unit":"c","city":"Shanghai"}}}`))
	if err != nil {
		t.Fatalf("mock hit POST: %v", err)
	}
	mockBody, _ := io.ReadAll(rMockHit.Body)
	rMockHit.Body.Close()
	if !strings.Contains(string(mockBody), "clear 25C") || upstreamHits.Load() != hitsBeforeMock {
		t.Fatalf("expected offline mock hit without hitting upstream, body=%s", string(mockBody))
	}

	// 未录制入参应返回 -32004 错误。
	rMockMiss, err := mockSrv.Client().Post(mockSrv.URL, "application/json",
		strings.NewReader(`{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"get_weather","arguments":{"city":"Unknown"}}}`))
	if err != nil {
		t.Fatalf("mock miss POST: %v", err)
	}
	missBody, _ := io.ReadAll(rMockMiss.Body)
	rMockMiss.Body.Close()
	if !strings.Contains(string(missBody), "-32004") {
		t.Fatalf("expected -32004 on strict mock miss, got %s", string(missBody))
	}
	mockSrv.Client().CloseIdleConnections()
	mockSrv.Close()

	// 3. 工具侧 5 类混沌故障验证（timeout, jsonrpc_error, tool_is_error, malformed_json, delay）。
	actions := []string{"timeout", "jsonrpc_error", "tool_is_error", "malformed_json", "delay"}
	for _, act := range actions {
		rule := ChaosRule{Name: "rule_" + act, ToolName: "*", EveryN: 1, Action: act, DelayMs: 15, ErrorMessage: "boom-" + act}
		in := NewInterceptor(Options{
			Mode:  "chaos",
			Chaos: NewChaosEngine([]ChaosRule{rule}),
		})
		out, _, cErr := in.HandleToolCall(context.Background(), JSONRPCMessage{
			JSONRPC: "2.0",
			ID:      json.RawMessage(`9`),
			Method:  "tools/call",
			Params:  json.RawMessage(`{"name":"t","arguments":{}}`),
		}, []byte(`{}`), func(_ context.Context, _ []byte) ([]byte, error) {
			return []byte(`{"jsonrpc":"2.0","id":9,"result":{"ok":true}}`), nil
		})
		if cErr != nil {
			t.Fatalf("chaos %s failed: %v", act, cErr)
		}
		txt := string(out)
		switch act {
		case "timeout":
			if !strings.Contains(txt, "-32001") {
				t.Fatalf("timeout expected -32001, got %s", txt)
			}
		case "jsonrpc_error":
			if !strings.Contains(txt, "boom-jsonrpc_error") {
				t.Fatalf("jsonrpc_error unexpected: %s", txt)
			}
		case "tool_is_error":
			if !strings.Contains(txt, `"isError":true`) {
				t.Fatalf("tool_is_error unexpected: %s", txt)
			}
		case "malformed_json":
			if json.Valid(out) {
				t.Fatalf("expected invalid json for malformed_json")
			}
		case "delay":
			if !strings.Contains(txt, `"ok":true`) {
				t.Fatalf("delay expected forwarded response, got %s", txt)
			}
		}
	}
}

func TestStdioProxy_OfflineMockAndPassthrough(t *testing.T) {
	tape := NewTapeStore([]MCPRecord{{
		Method:         "tools/call",
		ToolName:       "calc_distance",
		ArgsHash:       ToolCallKey("calc_distance", json.RawMessage(`{"a":1,"b":2}`)),
		ResponseResult: json.RawMessage(`{"content":[{"type":"text","text":"42.5"}]}`),
	}})
	proxy := NewStdioProxy(NewInterceptor(Options{
		Mode:         "mock",
		MockFallback: "strict",
		MockTape:     tape,
	}), "", nil, "")

	inReader, inWriter := io.Pipe()
	var outBuf bytes.Buffer
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = proxy.ServeStreams(context.Background(), inReader, &outBuf)
	}()

	_, _ = io.WriteString(inWriter, `{"jsonrpc":"2.0","method":"notifications/initialized"}`+"\n")
	_, _ = io.WriteString(inWriter, `{"jsonrpc":"2.0","id":10,"method":"tools/call","params":{"name":"calc_distance","arguments":{"b":2,"a":1}}}`+"\n")
	_ = inWriter.Close()
	wg.Wait()

	if !strings.Contains(outBuf.String(), "42.5") || math.IsNaN(1) {
		t.Fatalf("unexpected stdio mock output: %s", outBuf.String())
	}
}
