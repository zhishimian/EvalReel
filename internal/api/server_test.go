package api

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"evalreel/internal/config"
	"evalreel/internal/scheduler"
	"evalreel/internal/store"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func newTestServer(t *testing.T, delayPerTask time.Duration) (*Server, *store.SQLiteStore) {
	t.Helper()
	st, err := store.NewSQLiteStore(filepath.Join(t.TempDir(), "api_test.db"))
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	broker := scheduler.NewProgressBroker()

	builder := func(_ context.Context, runID string, cfg *config.EvalConfig) ([]store.EvalTask, error) {
		var tasks []store.EvalTask
		for i := 0; i < 4; i++ {
			for _, tgt := range cfg.Targets {
				tasks = append(tasks, store.EvalTask{
					ID:         fmt.Sprintf("%s-%s-%d", runID, tgt.Name, i),
					RunID:      runID,
					ItemID:     fmt.Sprintf("item-%d", i),
					TargetName: tgt.Name,
					TrialIndex: 0,
					Status:     store.TaskPending,
				})
			}
		}
		return tasks, nil
	}

	launcher := func(ctx context.Context, runID string, cfg *config.EvalConfig) {
		sched := scheduler.New(scheduler.Config{
			Concurrency: cfg.Concurrency,
			MaxRetries:  cfg.MaxRetries,
			TaskTimeout: 5 * time.Second,
			BaseBackoff: 5 * time.Millisecond,
		}, st, nil, broker)
		_ = sched.Run(ctx, runID, func(_ context.Context, tk store.EvalTask) (store.TaskResultUpdate, []store.EvalScore, error) {
			if delayPerTask > 0 {
				time.Sleep(delayPerTask)
			}
			return store.TaskResultUpdate{
				Status:       store.TaskSuccess,
				ResponseText: "done-" + tk.ItemID,
				TotalMs:      5,
			}, nil, nil
		})
	}

	srv := NewServer(st, broker, builder, launcher)
	t.Cleanup(func() {
		srv.Shutdown()
		_ = st.Close()
	})
	return srv, st
}

func TestAPI_CreateRunAndQuery(t *testing.T) {
	srv, _ := newTestServer(t, 2*time.Millisecond)
	httpSrv := httptest.NewServer(srv.Handler())
	defer func() {
		httpSrv.Client().CloseIdleConnections()
		httpSrv.Close()
	}()

	payload := `{
		"name":"api-smoke",
		"mode":"benchmark",
		"concurrency":2,
		"targets":[{"name":"qwen3.8-flash","base_url":"http://127.0.0.1:9090/v1","model":"qwen3.8-flash"}]
	}`
	resp, err := httpSrv.Client().Post(httpSrv.URL+"/api/runs", "application/json", strings.NewReader(payload))
	if err != nil {
		t.Fatalf("POST /api/runs failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted, got %d", resp.StatusCode)
	}

	var createBody struct {
		RunID      string `json:"run_id"`
		TotalTasks int    `json:"total_tasks"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&createBody); err != nil {
		t.Fatalf("decode create response: %v", err)
	}
	if createBody.RunID == "" || createBody.TotalTasks != 4 {
		t.Fatalf("unexpected create response: %+v", createBody)
	}

	// 轮询直到后台任务完成
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		rResp, err := httpSrv.Client().Get(httpSrv.URL + "/api/runs/" + createBody.RunID)
		if err != nil {
			t.Fatalf("GET /api/runs/:id: %v", err)
		}
		var detail struct {
			Run struct {
				Status    store.RunStatus `json:"status"`
				DoneTasks int             `json:"done_tasks"`
			} `json:"run"`
			Tasks []store.EvalTask `json:"tasks"`
		}
		_ = json.NewDecoder(rResp.Body).Decode(&detail)
		rResp.Body.Close()

		if detail.Run.Status == store.RunCompleted {
			if detail.Run.DoneTasks != 4 || len(detail.Tasks) != 4 {
				t.Fatalf("unexpected detail: %+v", detail)
			}
			break
		}
		time.Sleep(15 * time.Millisecond)
	}

	listResp, err := httpSrv.Client().Get(httpSrv.URL + "/api/runs?limit=5")
	if err != nil {
		t.Fatalf("GET /api/runs: %v", err)
	}
	defer listResp.Body.Close()
	var listBody struct {
		Runs []store.EvalRun `json:"runs"`
	}
	if err := json.NewDecoder(listResp.Body).Decode(&listBody); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if len(listBody.Runs) != 1 || listBody.Runs[0].Status != store.RunCompleted {
		t.Fatalf("unexpected list result: %+v", listBody)
	}
}

func TestAPI_ValidationAnd404(t *testing.T) {
	srv, _ := newTestServer(t, 0)
	httpSrv := httptest.NewServer(srv.Handler())
	defer func() {
		httpSrv.Client().CloseIdleConnections()
		httpSrv.Close()
	}()

	// 缺少 targets 应返回 400
	resp, err := httpSrv.Client().Post(httpSrv.URL+"/api/runs", "application/json", strings.NewReader(`{"name":"bad"}`))
	if err != nil {
		t.Fatalf("POST invalid: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}

	// 不存在的 run_id 应返回 404
	r404, err := httpSrv.Client().Get(httpSrv.URL + "/api/runs/not_exists")
	if err != nil {
		t.Fatalf("GET missing: %v", err)
	}
	r404.Body.Close()
	if r404.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", r404.StatusCode)
	}
}

func TestAPI_SSEProgressStream(t *testing.T) {
	srv, _ := newTestServer(t, 15*time.Millisecond)
	httpSrv := httptest.NewServer(srv.Handler())
	defer func() {
		httpSrv.Client().CloseIdleConnections()
		httpSrv.Close()
	}()

	payload := `{"name":"sse-test","mode":"benchmark","concurrency":1,"targets":[{"name":"m1","base_url":"http://127.0.0.1:9090/v1","model":"m1"}]}`
	resp, err := httpSrv.Client().Post(httpSrv.URL+"/api/runs", "application/json", strings.NewReader(payload))
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	var createBody struct {
		RunID string `json:"run_id"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&createBody)
	resp.Body.Close()

	evResp, err := httpSrv.Client().Get(httpSrv.URL + "/api/runs/" + createBody.RunID + "/events")
	if err != nil {
		t.Fatalf("GET SSE failed: %v", err)
	}
	defer evResp.Body.Close()

	sc := bufio.NewScanner(evResp.Body)
	var sawCompleted bool
	var eventsCount int
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "data:") {
			eventsCount++
			if strings.Contains(line, `"completed"`) {
				sawCompleted = true
			}
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("SSE scanner err: %v", err)
	}
	if !sawCompleted || eventsCount == 0 {
		t.Fatalf("expected completed SSE event, got count=%d completed=%v", eventsCount, sawCompleted)
	}
}

func TestAPI_ReportDownload(t *testing.T) {
	srv, st := newTestServer(t, 0)
	httpSrv := httptest.NewServer(srv.Handler())
	defer func() {
		httpSrv.Client().CloseIdleConnections()
		httpSrv.Close()
	}()

	// 报告未生成前应返回 404
	r404, err := httpSrv.Client().Get(httpSrv.URL + "/api/runs/r-1/report")
	if err != nil {
		t.Fatalf("GET report 404: %v", err)
	}
	r404.Body.Close()
	if r404.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", r404.StatusCode)
	}

	if err := st.SaveReport(context.Background(), &store.EvalReport{
		RunID:       "r-1",
		SummaryJSON: `{}`,
		HTMLContent: "<html><body>EvalReel Report</body></html>",
	}); err != nil {
		t.Fatalf("SaveReport: %v", err)
	}

	r200, err := httpSrv.Client().Get(httpSrv.URL + "/api/runs/r-1/report")
	if err != nil {
		t.Fatalf("GET report 200: %v", err)
	}
	defer r200.Body.Close()
	body, _ := io.ReadAll(r200.Body)
	if r200.StatusCode != http.StatusOK || !strings.Contains(string(body), "EvalReel Report") {
		t.Fatalf("unexpected report response: code=%d body=%s", r200.StatusCode, string(body))
	}
}

func TestAPI_IdempotencyKey(t *testing.T) {
	st, err := store.NewSQLiteStore(filepath.Join(t.TempDir(), "idem_test.db"))
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	var launchCount atomic.Int32
	srv := NewServer(
		st,
		scheduler.NewProgressBroker(),
		func(_ context.Context, runID string, _ *config.EvalConfig) ([]store.EvalTask, error) {
			return []store.EvalTask{{ID: runID + "-t0", RunID: runID, ItemID: "i0", TargetName: "m1", Status: store.TaskPending}}, nil
		},
		func(_ context.Context, _ string, _ *config.EvalConfig) {
			launchCount.Add(1)
		},
	)
	t.Cleanup(func() {
		srv.Shutdown()
		_ = st.Close()
	})

	httpSrv := httptest.NewServer(srv.Handler())
	defer func() {
		httpSrv.Client().CloseIdleConnections()
		httpSrv.Close()
	}()

	payload := `{"name":"idem-test","mode":"benchmark","targets":[{"name":"m1","base_url":"http://127.0.0.1:9090/v1","model":"m1"}]}`
	postWithKey := func(key string) (int, string, bool) {
		req, _ := http.NewRequest(http.MethodPost, httpSrv.URL+"/api/runs", strings.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		if key != "" {
			req.Header.Set("Idempotency-Key", key)
		}
		resp, err := httpSrv.Client().Do(req)
		if err != nil {
			t.Fatalf("POST failed: %v", err)
		}
		defer resp.Body.Close()
		var body struct {
			RunID            string `json:"run_id"`
			IdempotentReplay bool   `json:"idempotent_replay"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&body)
		return resp.StatusCode, body.RunID, body.IdempotentReplay
	}

	// 1. 同一个幂等键首次提交返回 202，重放返回 200 并复用同一 run_id。
	c1, id1, r1 := postWithKey("ci-req-123")
	if c1 != http.StatusAccepted || id1 == "" || r1 {
		t.Fatalf("first request unexpected: code=%d id=%s replay=%v", c1, id1, r1)
	}
	c2, id2, r2 := postWithKey("ci-req-123")
	if c2 != http.StatusOK || id2 != id1 || !r2 {
		t.Fatalf("replay request unexpected: code=%d id=%s want=%s replay=%v", c2, id2, id1, r2)
	}

	// 2. 并发重放同一个幂等键时只会启动一次。
	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			code, runID, _ := postWithKey("ci-conc-456")
			if code != http.StatusAccepted && code != http.StatusOK {
				t.Errorf("unexpected status code %d", code)
			}
			if runID == "" {
				t.Errorf("empty run_id")
			}
		}()
	}
	wg.Wait()

	// 3. 不带幂等键的两次请求必须创建两个不同的 run_id。
	_, noKey1, _ := postWithKey("")
	_, noKey2, _ := postWithKey("")
	if noKey1 == "" || noKey2 == "" || noKey1 == noKey2 {
		t.Fatalf("expected distinct run ids without idempotency key, got %q and %q", noKey1, noKey2)
	}

	if launchCount.Load() != 4 {
		t.Fatalf("expected exactly 4 launched runs (2 keyed + 2 unkeyed), got %d", launchCount.Load())
	}
}
