package runner

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"evalreel/internal/config"
	"evalreel/internal/mockllm"
	"evalreel/internal/model"
	"evalreel/internal/recorder"
	"evalreel/internal/scheduler"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestLoadDataset_TestFixtures(t *testing.T) {
	heItems, err := LoadDataset(filepath.Join("..", "..", "testdata", "humaneval_mini.jsonl"))
	if err != nil {
		t.Fatalf("LoadDataset humaneval: %v", err)
	}
	if len(heItems) != 10 || heItems[0].EntryPoint != "has_close_elements" {
		t.Fatalf("unexpected humaneval items: len=%d first=%+v", len(heItems), heItems[0])
	}

	tcItems, err := LoadDataset(filepath.Join("..", "..", "testdata", "toolcall_mini.jsonl"))
	if err != nil {
		t.Fatalf("LoadDataset toolcall: %v", err)
	}
	if len(tcItems) != 10 || len(tcItems[0].Tools) == 0 {
		t.Fatalf("unexpected toolcall items: len=%d first=%+v", len(tcItems), tcItems[0])
	}
}

func TestLLMRunner_StreamAndRetry429(t *testing.T) {
	cfg := mockllm.DefaultConfig()
	cfg.TTFT = 10 * time.Millisecond
	cfg.ChunkInterval = 5 * time.Millisecond
	srv := httptest.NewServer(mockllm.NewServer(cfg).Handler())
	defer srv.Close()

	r := NewLLMRunner()
	defer r.CloseIdleConnections()

	target := config.TargetConfig{
		Name:             "mock-1",
		BaseURL:          srv.URL + "/v1",
		Model:            "mock-reasoning-v1",
		InputPricePer1M:  0.8,
		OutputPricePer1M: 2.7,
	}

	// 1. 正常流式请求
	out, err := r.Execute(context.Background(), target, DatasetItem{
		ID:     "q1",
		Prompt: "什么是空间索引",
	})
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if out.ReasoningText == "" || out.ResponseText == "" || out.InputTokens != 20 || out.OutputTokens != 32 {
		t.Fatalf("unexpected output: %+v", out)
	}
	if out.CostUSD <= 0 {
		t.Fatalf("expected positive cost, got %f", out.CostUSD)
	}

	// 2. 上游返回 429 时必须包装成可重试错误并带 RetryAfter
	srv429 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "3")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":{"message":"too many requests"}}`))
	}))
	defer srv429.Close()

	target429 := target
	target429.BaseURL = srv429.URL + "/v1"
	_, err = r.Execute(context.Background(), target429, DatasetItem{ID: "q2", Prompt: "hi"})
	retryAfter, retryable := scheduler.IsRetryable(err)
	if !retryable || retryAfter != 3*time.Second {
		t.Fatalf("expected RetryableError with 3s RetryAfter, got err=%v retryable=%v after=%v", err, retryable, retryAfter)
	}
}

func TestLoadForkItem_FromGzipTrace(t *testing.T) {
	tracePath := filepath.Join(t.TempDir(), "fork_src.jsonl.gz")
	rec, err := recorder.NewGzipJSONLRecorder(tracePath)
	if err != nil {
		t.Fatalf("NewGzipJSONLRecorder: %v", err)
	}

	reqJSON := json.RawMessage(`{"model":"m1","messages":[{"role":"user","content":"先查北京坐标"},{"role":"assistant","tool_calls":[{"id":"c1","type":"function","function":{"name":"get_city_coords","arguments":"{\"city\":\"北京\"}"}}]},{"role":"tool","tool_call_id":"c1","content":"{\"lat\":39.9,\"lon\":116.4}"}],"tools":[{"type":"function","function":{"name":"calc_distance","parameters":{"type":"object"}}}]}`)
	_ = rec.Write(model.TraceRecord{TraceID: "tr-0", RequestBody: json.RawMessage(`{"messages":[{"role":"user","content":"step0"}]}`)})
	_ = rec.Write(model.TraceRecord{TraceID: "tr-1", RequestBody: reqJSON})
	if err := rec.Close(); err != nil {
		t.Fatalf("rec.Close: %v", err)
	}

	item, err := LoadForkItem(tracePath, "tr-1", 0)
	if err != nil {
		t.Fatalf("LoadForkItem: %v", err)
	}
	if len(item.Messages) == 0 || len(item.Tools) == 0 {
		t.Fatalf("expected messages and tools extracted, got %+v", item)
	}

	// 将抽出来的第 2 步上下文喂给新目标执行
	cfg := mockllm.DefaultConfig()
	cfg.TTFT = 5 * time.Millisecond
	cfg.ChunkInterval = 2 * time.Millisecond
	srv := httptest.NewServer(mockllm.NewServer(cfg).Handler())
	defer srv.Close()

	r := NewLLMRunner()
	defer r.CloseIdleConnections()

	out, err := r.Execute(context.Background(), config.TargetConfig{
		Name:    "new-model",
		BaseURL: srv.URL + "/v1",
		Model:   "mock-reasoning-v1",
	}, item)
	if err != nil || out.ResponseText == "" {
		t.Fatalf("forked Execute failed: out=%+v err=%v", out, err)
	}
}

func TestAgentRunner_IsolatedWorkDirAndTimeoutKill(t *testing.T) {
	// 1. 验证在独立临时目录里创建产物文件，不污染仓库目录
	var cmdName string
	var args []string
	if runtime.GOOS == "windows" {
		cmdName = "cmd"
		args = []string{"/c", "echo evalreel_ok > artifact.txt"}
	} else {
		cmdName = "sh"
		args = []string{"-c", "echo evalreel_ok > artifact.txt"}
	}

	ar := NewAgentRunner(config.AgentConfig{
		Command:    cmdName,
		Args:       args,
		TimeoutSec: 5,
	})
	out, err := ar.Execute(context.Background(), config.TargetConfig{Name: "local"}, DatasetItem{ID: "a1"})
	if err != nil {
		t.Fatalf("AgentRunner Execute failed: %v", err)
	}
	defer os.RemoveAll(out.WorkDir)

	if _, err := os.Stat(filepath.Join(out.WorkDir, "artifact.txt")); err != nil {
		t.Fatalf("expected artifact.txt inside isolated workdir %s: %v", out.WorkDir, err)
	}

	// 2. 验证超时强杀子进程树
	var slowCmd string
	var slowArgs []string
	if runtime.GOOS == "windows" {
		slowCmd = "powershell"
		slowArgs = []string{"-NoProfile", "-Command", "Start-Sleep -Seconds 10"}
	} else {
		slowCmd = "sh"
		slowArgs = []string{"-c", "sleep 10"}
	}

	slowRunner := NewAgentRunner(config.AgentConfig{
		Command: slowCmd,
		Args:    slowArgs,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	start := time.Now()
	slowOut, err := slowRunner.Execute(ctx, config.TargetConfig{Name: "slow"}, DatasetItem{ID: "a2"})
	_ = os.RemoveAll(slowOut.WorkDir)
	if err == nil {
		t.Fatalf("expected timeout error from slow agent")
	}
	if time.Since(start) > 3*time.Second {
		t.Fatalf("expected fast process-tree kill, took %v", time.Since(start))
	}
}
