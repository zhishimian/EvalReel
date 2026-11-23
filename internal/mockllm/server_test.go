package mockllm

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestModelsEndpoint(t *testing.T) {
	srv := httptest.NewServer(NewServer(DefaultConfig()).Handler())
	defer srv.Close()

	resp, err := srv.Client().Get(srv.URL + "/v1/models")
	if err != nil {
		t.Fatalf("GET /v1/models failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	var body struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode models failed: %v", err)
	}
	if len(body.Data) == 0 || body.Data[0].ID != "mock-reasoning-v1" {
		t.Fatalf("unexpected models payload: %+v", body)
	}
}

func TestChatCompletionsStream(t *testing.T) {
	cfg := DefaultConfig()
	cfg.TTFT = 10 * time.Millisecond
	cfg.ChunkInterval = 5 * time.Millisecond
	srv := httptest.NewServer(NewServer(cfg).Handler())
	defer srv.Close()

	payload := `{"model":"mock-reasoning-v1","stream":true,"stream_options":{"include_usage":true},"messages":[{"role":"user","content":"hi"}]}`
	resp, err := srv.Client().Post(srv.URL+"/v1/chat/completions", "application/json", strings.NewReader(payload))
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body failed: %v", err)
	}
	text := string(raw)
	for _, want := range []string{`"reasoning_content"`, `"content"`, `"usage"`, `data: [DONE]`} {
		if !strings.Contains(text, want) {
			t.Fatalf("stream response missing %s:\n%s", want, text)
		}
	}
}

func TestChatCompletionsNonStream(t *testing.T) {
	cfg := DefaultConfig()
	cfg.TTFT = 5 * time.Millisecond
	srv := httptest.NewServer(NewServer(cfg).Handler())
	defer srv.Close()

	payload := `{"model":"mock-reasoning-v1","stream":false,"messages":[{"role":"user","content":"hi"}]}`
	resp, err := srv.Client().Post(srv.URL+"/v1/chat/completions", "application/json", strings.NewReader(payload))
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	var parsed struct {
		Choices []struct {
			Message struct {
				ReasoningContent string `json:"reasoning_content"`
				Content          string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
		Usage struct {
			TotalTokens int `json:"total_tokens"`
		} `json:"usage"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if len(parsed.Choices) != 1 || parsed.Choices[0].Message.ReasoningContent == "" || parsed.Choices[0].Message.Content == "" {
		t.Fatalf("unexpected non-stream payload: %+v", parsed)
	}
	if parsed.Usage.TotalTokens == 0 {
		t.Fatalf("expected non-zero usage")
	}
}

func TestChatCompletionsFault429(t *testing.T) {
	srv := httptest.NewServer(NewServer(DefaultConfig()).Handler())
	defer srv.Close()

	req, err := http.NewRequest(http.MethodPost, srv.URL+"/v1/chat/completions", strings.NewReader(`{"stream":true}`))
	if err != nil {
		t.Fatalf("new request failed: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Mock-Fault", "429")

	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("expected 429, got %d", resp.StatusCode)
	}
	if resp.Header.Get("Retry-After") == "" {
		t.Fatalf("expected Retry-After header on 429")
	}
}

func TestChatCompletionsClientCancel(t *testing.T) {
	var canceled atomic.Bool
	cancelCh := make(chan struct{}, 1)
	cfg := Config{
		TTFT:          500 * time.Millisecond,
		ChunkInterval: 50 * time.Millisecond,
		ModelName:     "mock-reasoning-v1",
		OnCanceled: func() {
			if canceled.CompareAndSwap(false, true) {
				close(cancelCh)
			}
		},
	}
	srv := httptest.NewServer(NewServer(cfg).Handler())
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/v1/chat/completions", strings.NewReader(`{"stream":true}`))
	if err != nil {
		t.Fatalf("new request failed: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	go func() {
		time.Sleep(40 * time.Millisecond)
		cancel()
	}()

	resp, err := srv.Client().Do(req)
	if err == nil {
		resp.Body.Close()
	}

	select {
	case <-cancelCh:
	case <-time.After(150 * time.Millisecond):
		t.Fatalf("expected mockllm to observe client cancel promptly")
	}
}

func TestChatCompletionsToolCallsFragmented(t *testing.T) {
	cfg := DefaultConfig()
	cfg.TTFT = 5 * time.Millisecond
	cfg.ChunkInterval = 2 * time.Millisecond
	srv := httptest.NewServer(NewServer(cfg).Handler())
	defer srv.Close()

	req, err := http.NewRequest(http.MethodPost, srv.URL+"/v1/chat/completions", strings.NewReader(`{"stream":true}`))
	if err != nil {
		t.Fatalf("new request failed: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Mock-Fault", "tool_calls")

	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	scanner := bufio.NewScanner(resp.Body)
	var assembledArgs strings.Builder
	var fragCount int
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data: ") || line == "data: [DONE]" {
			continue
		}
		var chunk struct {
			Choices []struct {
				Delta struct {
					ToolCalls []struct {
						Function struct {
							Arguments string `json:"arguments"`
						} `json:"function"`
					} `json:"tool_calls"`
				} `json:"delta"`
			} `json:"choices"`
		}
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &chunk); err != nil {
			t.Fatalf("unmarshal failed: %v", err)
		}
		if len(chunk.Choices) > 0 && len(chunk.Choices[0].Delta.ToolCalls) > 0 {
			fragCount++
			assembledArgs.WriteString(chunk.Choices[0].Delta.ToolCalls[0].Function.Arguments)
		}
	}
	if fragCount != 5 {
		t.Fatalf("expected 5 tool call fragments, got %d", fragCount)
	}
	if assembledArgs.String() != `{"city":"beijing","unit":"celsius"}` {
		t.Fatalf("unexpected assembled args: %s", assembledArgs.String())
	}
}

func TestChatCompletionsDirtyFaults(t *testing.T) {
	cfg := DefaultConfig()
	cfg.TTFT = 5 * time.Millisecond
	cfg.ChunkInterval = 2 * time.Millisecond
	srv := httptest.NewServer(NewServer(cfg).Handler())
	defer srv.Close()

	tests := []struct {
		name      string
		fault     string
		checkBody func(t *testing.T, body string, readErr error)
	}{
		{
			name:  "oversized_chunk_over_64kb",
			fault: "oversized_chunk",
			checkBody: func(t *testing.T, body string, readErr error) {
				if readErr != nil {
					t.Fatalf("unexpected read err: %v", readErr)
				}
				if len(body) <= 128*1024 {
					t.Fatalf("expected oversized body > 128KB, got %d", len(body))
				}
			},
		},
		{
			name:  "mid_stream_error",
			fault: "mid_stream_error",
			checkBody: func(t *testing.T, body string, readErr error) {
				if readErr != nil {
					t.Fatalf("unexpected read err: %v", readErr)
				}
				if !strings.Contains(body, `"upstream overloaded mid-stream"`) {
					t.Fatalf("missing mid-stream error in body: %s", body)
				}
			},
		},
		{
			name:  "no_done",
			fault: "no_done",
			checkBody: func(t *testing.T, body string, readErr error) {
				if readErr != nil {
					t.Fatalf("unexpected read err: %v", readErr)
				}
				if strings.Contains(body, "[DONE]") {
					t.Fatalf("expected no [DONE] marker, got: %s", body)
				}
			},
		},
		{
			name:  "cut_after_3",
			fault: "cut_after_3",
			checkBody: func(t *testing.T, body string, readErr error) {
				if strings.Contains(body, "[DONE]") {
					t.Fatalf("expected abrupt cut without [DONE]")
				}
				if !strings.Contains(body, "片段3") {
					t.Fatalf("expected at least 3 chunks before cut, got: %s", body)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodPost, srv.URL+"/v1/chat/completions", strings.NewReader(`{"stream":true}`))
			if err != nil {
				t.Fatalf("new request failed: %v", err)
			}
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-Mock-Fault", tc.fault)

			resp, err := srv.Client().Do(req)
			if err != nil {
				t.Fatalf("request failed: %v", err)
			}
			defer resp.Body.Close()

			raw, readErr := io.ReadAll(resp.Body)
			tc.checkBody(t, string(raw), readErr)
		})
	}
}
