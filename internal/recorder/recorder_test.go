package recorder

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"evalreel/internal/model"
)

func TestGzipJSONLRecorder_WriteAndRead(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "traces", "session_test.jsonl.gz")
	rec, err := NewGzipJSONLRecorder(outPath)
	if err != nil {
		t.Fatalf("NewGzipJSONLRecorder failed: %v", err)
	}

	want := model.TraceRecord{
		TraceID:       "trace-001",
		SessionID:     "sess-001",
		Timestamp:     time.Date(2026, 11, 23, 14, 0, 0, 0, time.UTC),
		Method:        "POST",
		Path:          "/v1/chat/completions",
		StatusCode:    200,
		Status:        model.StatusOK,
		RequestModel:  "mock-reasoning-v1",
		ResponseModel: "mock-reasoning-v1",
		ReasoningText: "先分析问题",
		ContentText:   "这是测试答案",
		ToolCalls: []model.ToolCall{
			{
				Index:     0,
				ID:        "call_1",
				Type:      "function",
				Name:      "get_weather",
				Arguments: `{"city":"beijing"}`,
			},
		},
		FinishReason: "stop",
		Metrics: model.TraceMetrics{
			TTFTReasoningMs: 120,
			TTFTAnswerMs:    350,
			TotalMs:         800,
			TPS:             42.5,
			InputTokens:     20,
			OutputTokens:    34,
		},
	}

	if err := rec.Write(want); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := rec.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	got := readAllTraces(t, outPath)
	if len(got) != 1 {
		t.Fatalf("expected 1 record, got %d", len(got))
	}
	if got[0].TraceID != want.TraceID || got[0].ReasoningText != want.ReasoningText || got[0].ContentText != want.ContentText {
		t.Fatalf("mismatch record: got %+v, want %+v", got[0], want)
	}
	if len(got[0].ToolCalls) != 1 || got[0].ToolCalls[0].Arguments != want.ToolCalls[0].Arguments {
		t.Fatalf("mismatch tool calls: %+v", got[0].ToolCalls)
	}
}

func TestGzipJSONLRecorder_ConcurrentWrite(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "concurrent.jsonl.gz")
	rec, err := NewGzipJSONLRecorder(outPath)
	if err != nil {
		t.Fatalf("NewGzipJSONLRecorder failed: %v", err)
	}

	const workers = 50
	const perWorker = 20
	var wg sync.WaitGroup
	wg.Add(workers)

	for w := 0; w < workers; w++ {
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				err := rec.Write(model.TraceRecord{
					TraceID:     fmt.Sprintf("w%d-i%d", workerID, i),
					SessionID:   "concurrent-sess",
					Method:      "POST",
					Path:        "/v1/chat/completions",
					StatusCode:  200,
					Status:      model.StatusOK,
					ContentText: "并发写入内容验证",
				})
				if err != nil {
					t.Errorf("concurrent Write error: %v", err)
				}
			}
		}(w)
	}
	wg.Wait()

	if err := rec.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	got := readAllTraces(t, outPath)
	if len(got) != workers*perWorker {
		t.Fatalf("expected %d records, got %d", workers*perWorker, len(got))
	}
}

func TestGzipJSONLRecorder_CloseBehavior(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "close.jsonl.gz")
	rec, err := NewGzipJSONLRecorder(outPath)
	if err != nil {
		t.Fatalf("NewGzipJSONLRecorder failed: %v", err)
	}

	if err := rec.Close(); err != nil {
		t.Fatalf("first Close failed: %v", err)
	}
	if err := rec.Close(); err != nil {
		t.Fatalf("second Close should be no-op, got: %v", err)
	}

	err = rec.Write(model.TraceRecord{TraceID: "after-close"})
	if !errors.Is(err, ErrRecorderClosed) {
		t.Fatalf("expected ErrRecorderClosed, got: %v", err)
	}
}

func readAllTraces(t *testing.T, path string) []model.TraceRecord {
	t.Helper()

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()

	gz, err := gzip.NewReader(f)
	if err != nil {
		t.Fatalf("gzip NewReader: %v", err)
	}
	defer gz.Close()

	var out []model.TraceRecord
	scanner := bufio.NewScanner(gz)
	for scanner.Scan() {
		var item model.TraceRecord
		if err := json.Unmarshal(scanner.Bytes(), &item); err != nil {
			t.Fatalf("unmarshal line: %v", err)
		}
		out = append(out, item)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scanner error: %v", err)
	}
	return out
}
