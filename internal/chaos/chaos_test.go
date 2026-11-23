package chaos

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"evalreel/internal/config"
)

func TestPreForward_EveryN429(t *testing.T) {
	eng := NewEngine(config.ChaosConfig{
		Enabled: true,
		Rules: []config.ChaosRule{
			{
				Name:          "rate_limit_every_2",
				MatchPath:     "/v1/chat/completions",
				EveryN:        2,
				Action:        "status_429",
				RetryAfterSec: 3,
			},
		},
	})

	interceptCount := 0
	handler := eng.PreForward(
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, "ok")
		}),
		nil,
		func(_ *http.Request, rule config.ChaosRule, statusCode int) {
			interceptCount++
			if rule.Name != "rate_limit_every_2" || statusCode != 429 {
				t.Fatalf("unexpected callback args: rule=%+v status=%d", rule, statusCode)
			}
		},
	)

	var statuses []int
	for i := 1; i <= 6; i++ {
		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		statuses = append(statuses, rec.Code)
		if rec.Code == http.StatusTooManyRequests && rec.Header().Get("Retry-After") != "3" {
			t.Fatalf("req %d expected Retry-After=3, got %q", i, rec.Header().Get("Retry-After"))
		}
	}

	want := []int{200, 429, 200, 429, 200, 429}
	for i := range want {
		if statuses[i] != want[i] {
			t.Fatalf("expected statuses %v, got %v", want, statuses)
		}
	}
	if interceptCount != 3 {
		t.Fatalf("expected 3 intercepts, got %d", interceptCount)
	}
}

func TestPreForward_DelayAndCancel(t *testing.T) {
	eng := NewEngine(config.ChaosConfig{
		Enabled: true,
		Rules: []config.ChaosRule{
			{
				Name:      "slow_delay",
				MatchPath: "/v1/chat/completions",
				EveryN:    1,
				Action:    "delay",
				DelayMs:   200,
			},
		},
	})

	nextCalled := false
	handler := eng.PreForward(
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			nextCalled = true
			w.WriteHeader(http.StatusOK)
		}),
		nil,
		nil,
	)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil).WithContext(ctx)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if nextCalled {
		t.Fatalf("expected canceled delay request not to reach upstream")
	}
}

func TestWrapStream_CutAfter5Chunks(t *testing.T) {
	var sb strings.Builder
	for i := 1; i <= 8; i++ {
		fmt.Fprintf(&sb, "data: {\"chunk\":%d}\n\n", i)
	}

	r := WrapStream(io.NopCloser(strings.NewReader(sb.String())), &config.ChaosRule{
		Name:           "cut5",
		Action:         "cut_stream",
		CutAfterChunks: 5,
	})
	defer r.Close()

	// 使用小缓冲区逐次读取，顺便验证跨缓冲区的 \n\n 换行边界计数是否准确
	buf := make([]byte, 7)
	var collected strings.Builder
	var finalErr error
	for {
		n, err := r.Read(buf)
		if n > 0 {
			collected.Write(buf[:n])
		}
		if err != nil {
			finalErr = err
			break
		}
	}

	if !errors.Is(finalErr, ErrChaosStreamCut) {
		t.Fatalf("expected ErrChaosStreamCut, got %v", finalErr)
	}
	text := collected.String()
	if !strings.Contains(text, `"chunk":5`) || strings.Contains(text, `"chunk":6`) {
		t.Fatalf("expected exactly 5 chunks before cut, got:\n%s", text)
	}
}

func TestWrapStream_MalformedJSON(t *testing.T) {
	stream := "data: {\"chunk\":1}\n\ndata: {\"chunk\":2}\n\ndata: {\"chunk\":3}\n\n"
	r := WrapStream(io.NopCloser(strings.NewReader(stream)), &config.ChaosRule{
		Name:           "bad_json",
		Action:         "malformed_json",
		CutAfterChunks: 2,
	})
	defer r.Close()

	raw, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	text := string(raw)
	if !strings.Contains(text, `"chunk":2`) || strings.Contains(text, `"chunk":3`) {
		t.Fatalf("expected cut after chunk 2, got:\n%s", text)
	}
	if !strings.Contains(text, `data: {"choices":[{"delta":{"content":`) {
		t.Fatalf("expected malformed json chunk injected, got:\n%s", text)
	}
}
