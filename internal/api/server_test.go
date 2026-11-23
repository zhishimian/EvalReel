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
