package sse

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"evalreel/internal/model"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestParseStream_DirtyScenarios(t *testing.T) {
	t.Run("oversized_line_over_64kb", func(t *testing.T) {
		bigContent := strings.Repeat("x", 128*1024)
		stream := fmt.Sprintf("data: {\"model\":\"m1\",\"choices\":[{\"delta\":{\"content\":\"%s\"}}]}\n\ndata: [DONE]\n\n", bigContent)

		res := ParseStream(strings.NewReader(stream), ParseOptions{
			StartTime:      time.Now(),
			RequestText:    "hello",
			EstimateTokens: HeuristicEstimateTokens,
		})
		if res.Status != model.StatusOK {
			t.Fatalf("expected StatusOK, got %s (%s)", res.Status, res.RawError)
		}
		if len(res.ContentText) != 128*1024 {
			t.Fatalf("expected content length %d, got %d", 128*1024, len(res.ContentText))
		}
	})

	t.Run("reasoning_and_content_isolated", func(t *testing.T) {
		pr, pw := io.Pipe()
		go func() {
			defer pw.Close()
			for i := 0; i < 10; i++ {
				_, _ = fmt.Fprintf(pw, "data: {\"model\":\"r1\",\"choices\":[{\"delta\":{\"reasoning_content\":\"思%d\"}}]}\n\n", i)
			}
			time.Sleep(55 * time.Millisecond)
			for i := 0; i < 5; i++ {
				_, _ = fmt.Fprintf(pw, "data: {\"model\":\"r1\",\"choices\":[{\"delta\":{\"content\":\"答%d\"}}]}\n\n", i)
			}
			_, _ = io.WriteString(pw, "data: [DONE]\n\n")
		}()

		res := ParseStream(pr, ParseOptions{
			StartTime:      time.Now(),
			RequestText:    "question",
			EstimateTokens: HeuristicEstimateTokens,
		})
		if res.Status != model.StatusOK {
			t.Fatalf("expected StatusOK, got %s (%s)", res.Status, res.RawError)
		}
		if strings.Contains(res.ReasoningText, "答") || strings.Contains(res.ContentText, "思") {
			t.Fatalf("reasoning and content leaked: reasoning=%q content=%q", res.ReasoningText, res.ContentText)
		}
		if res.Metrics.TTFTAnswerMs < res.Metrics.TTFTReasoningMs+40 {
			t.Fatalf("expected answer TTFT >= reasoning TTFT + 40ms, got reasoning=%d answer=%d",
				res.Metrics.TTFTReasoningMs, res.Metrics.TTFTAnswerMs)
		}
	})

	t.Run("missing_done_and_usage", func(t *testing.T) {
		stream := "data: {\"model\":\"m1\",\"choices\":[{\"delta\":{\"content\":\"hello world\"},\"finish_reason\":\"stop\"}]}\n\n"
		res := ParseStream(strings.NewReader(stream), ParseOptions{
			StartTime:      time.Now().Add(-20 * time.Millisecond),
			RequestText:    "hi there",
			EstimateTokens: HeuristicEstimateTokens,
		})
		if res.Status != model.StatusOK {
			t.Fatalf("expected StatusOK without [DONE], got %s (%s)", res.Status, res.RawError)
		}
		if !res.Metrics.TokensEstimated {
			t.Fatalf("expected TokensEstimated=true when usage is missing")
		}
		if res.Metrics.InputTokens == 0 || res.Metrics.OutputTokens == 0 {
			t.Fatalf("expected non-zero estimated tokens, got in=%d out=%d", res.Metrics.InputTokens, res.Metrics.OutputTokens)
		}
	})

	t.Run("http200_mid_stream_error", func(t *testing.T) {
		stream := strings.Join([]string{
			`data: {"model":"m1","choices":[{"delta":{"content":"part1 "}}]}`,
			`data: {"model":"m1","choices":[{"delta":{"content":"part2"}}]}`,
			`data: {"error":{"message":"rate limit"}}`,
			`data: [DONE]`,
			"",
		}, "\n\n")

		res := ParseStream(strings.NewReader(stream), ParseOptions{
			StartTime:      time.Now(),
			EstimateTokens: HeuristicEstimateTokens,
		})
		if res.Status != model.StatusError {
			t.Fatalf("expected StatusError, got %s", res.Status)
		}
		if res.RawError != "rate limit" {
			t.Fatalf("expected RawError 'rate limit', got %q", res.RawError)
		}
		if res.ContentText != "part1 part2" {
			t.Fatalf("expected partial content preserved, got %q", res.ContentText)
		}
	})

	t.Run("tool_calls_5_fragments", func(t *testing.T) {
		stream := strings.Join([]string{
			`data: {"model":"m1","choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"get_weather","arguments":"{\"city\":"}}]}}]}`,
			`data: {"model":"m1","choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"bei"}}]}}]}`,
			`data: {"model":"m1","choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"jing\","}}]}}]}`,
			`data: {"model":"m1","choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"unit\":"}}]}}]}`,
			`data: {"model":"m1","choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"celsius\"}"}}]},"finish_reason":"tool_calls"}]}`,
			`data: [DONE]`,
			"",
		}, "\n\n")

		res := ParseStream(strings.NewReader(stream), ParseOptions{
			StartTime:      time.Now(),
			EstimateTokens: HeuristicEstimateTokens,
		})
		if res.Status != model.StatusOK {
			t.Fatalf("expected StatusOK, got %s (%s)", res.Status, res.RawError)
		}
		if len(res.ToolCalls) != 1 {
			t.Fatalf("expected 1 assembled tool call, got %d", len(res.ToolCalls))
		}
		tc := res.ToolCalls[0]
		if tc.ID != "call_1" || tc.Name != "get_weather" || tc.Arguments != `{"city":"beijing","unit":"celsius"}` {
			t.Fatalf("unexpected assembled tool call: %+v", tc)
		}
	})
}

func TestTapReadCloser_FullStream(t *testing.T) {
	payload := strings.Join([]string{
		`data: {"model":"m1","choices":[{"delta":{"reasoning_content":"思考"}}]}`,
		`data: {"model":"m1","choices":[{"delta":{"content":"结果"},"finish_reason":"stop"}]}`,
		`data: {"model":"m1","choices":[],"usage":{"prompt_tokens":10,"completion_tokens":6,"total_tokens":16}}`,
		`data: [DONE]`,
		"",
	}, "\n\n")

	var (
		mu       sync.Mutex
		gotRes   Result
		callback bool
	)

	tap := NewTapReadCloser(
		context.Background(),
		io.NopCloser(strings.NewReader(payload)),
		ParseOptions{StartTime: time.Now(), EstimateTokens: HeuristicEstimateTokens},
		func(res Result) {
			mu.Lock()
			defer mu.Unlock()
			gotRes = res
			callback = true
		},
	)

	passedThrough, err := io.ReadAll(tap)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if err := tap.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if string(passedThrough) != payload {
		t.Fatalf("pass-through mismatch")
	}

	mu.Lock()
	defer mu.Unlock()
	if !callback {
		t.Fatalf("expected onComplete callback to run")
	}
	if gotRes.Status != model.StatusOK || gotRes.ReasoningText != "思考" || gotRes.ContentText != "结果" {
		t.Fatalf("unexpected parsed result: %+v", gotRes)
	}
	if gotRes.Metrics.InputTokens != 10 || gotRes.Metrics.OutputTokens != 6 || gotRes.Metrics.TokensEstimated {
		t.Fatalf("unexpected usage metrics: %+v", gotRes.Metrics)
	}
}

func TestTapReadCloser_EarlyCancel(t *testing.T) {
	pr, pw := io.Pipe()
	go func() {
		defer pw.Close()
		for i := 1; i <= 10; i++ {
			_, err := fmt.Fprintf(pw, "data: {\"model\":\"m1\",\"choices\":[{\"delta\":{\"content\":\"块%d \"}}]}\n\n", i)
			if err != nil {
				return
			}
			time.Sleep(15 * time.Millisecond)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	var gotRes Result
	var callback bool

	tap := NewTapReadCloser(
		ctx,
		pr,
		ParseOptions{StartTime: time.Now(), EstimateTokens: HeuristicEstimateTokens},
		func(res Result) {
			gotRes = res
			callback = true
		},
	)

	buf := make([]byte, 512)
	readCount := 0
	for readCount < 2 {
		n, err := tap.Read(buf)
		if n > 0 {
			readCount++
		}
		if err != nil {
			break
		}
	}

	cancel()
	if err := tap.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if !callback {
		t.Fatalf("expected onComplete callback on cancel")
	}
	if gotRes.Status != model.StatusAborted {
		t.Fatalf("expected StatusAborted, got %s", gotRes.Status)
	}
	if !strings.Contains(gotRes.ContentText, "块1") {
		t.Fatalf("expected partial content preserved, got %q", gotRes.ContentText)
	}
}

func TestTapReadCloser_CancelAfterDoneStaysOK(t *testing.T) {
	payload := strings.Join([]string{
		`data: {"model":"m1","choices":[{"delta":{"content":"完整回答"},"finish_reason":"stop"}]}`,
		`data: [DONE]`,
		"",
	}, "\n\n")

	ctx, cancel := context.WithCancel(context.Background())
	var gotRes Result

	tap := NewTapReadCloser(
		ctx,
		io.NopCloser(strings.NewReader(payload)),
		ParseOptions{StartTime: time.Now(), EstimateTokens: HeuristicEstimateTokens},
		func(res Result) {
			gotRes = res
		},
	)

	if _, err := io.ReadAll(tap); err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	// 模拟客户端在读完 [DONE] 后立刻关闭连接触发 ctx.Done()
	cancel()
	if err := tap.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if gotRes.Status != model.StatusOK || !gotRes.Completed {
		t.Fatalf("expected StatusOK and Completed=true, got status=%s completed=%v", gotRes.Status, gotRes.Completed)
	}
}
