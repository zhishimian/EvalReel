//go:build ignore

package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type runtimeSample struct {
	Goroutines int
	HeapMB     float64
}

func main() {
	serverURL := flag.String("server", "http://127.0.0.1:8081", "EvalReel Server 地址")
	runsCount := flag.Int("runs", 10, "并发提交的评测场次（每场 10 题 × 3 遍 = 30 条流式子任务）")
	trials := flag.Int("trials", 3, "每题重复次数")
	timeout := flag.Duration("timeout", 3*time.Minute, "总等待超时时间")
	flag.Parse()

	client := &http.Client{Timeout: 15 * time.Second}

	base := sampleMetrics(client, *serverURL+"/metrics")
	fmt.Printf("Baseline Server: goroutines=%d heap=%.2f MB\n", base.Goroutines, base.HeapMB)

	payloadMap := map[string]any{
		"name":         "v3_dist_loadtest",
		"mode":         "benchmark",
		"dataset_path": "/app/testdata/toolcall_mini.jsonl",
		"concurrency":  25,
		"trials":       *trials,
		"max_retries":  3,
		"targets": []map[string]any{{
			"name":      "mock-slow-5pct429",
			"base_url":  "http://mockllm:9090/v1",
			"model":     "mock-reasoning-v1",
			"rpm_limit": 0,
		}},
		"scorers": []map[string]any{{
			"type":    "rule",
			"name":    "contains_mock",
			"pattern": "分片",
		}},
		"output_dir": "/app/reports",
	}
	bodyBytes, _ := json.Marshal(payloadMap)

	startTime := time.Now()
	runIDs := make([]string, *runsCount)
	var idemReplayOK atomic.Bool

	var submitWG sync.WaitGroup
	for i := 0; i < *runsCount; i++ {
		submitWG.Add(1)
		go func(idx int) {
			defer submitWG.Done()
			req, _ := http.NewRequest(http.MethodPost, *serverURL+"/api/runs", bytes.NewReader(bodyBytes))
			req.Header.Set("Content-Type", "application/json")
			key := fmt.Sprintf("dist-load-%d-%d", startTime.UnixNano(), idx)
			req.Header.Set("Idempotency-Key", key)

			resp, err := client.Do(req)
			if err != nil {
				log.Fatalf("submit run %d failed: %v", idx, err)
			}
			var out struct {
				RunID string `json:"run_id"`
			}
			_ = json.NewDecoder(resp.Body).Decode(&out)
			resp.Body.Close()
			if resp.StatusCode != http.StatusAccepted || out.RunID == "" {
				log.Fatalf("unexpected POST %d status=%d", idx, resp.StatusCode)
			}
			runIDs[idx] = out.RunID

			// 对首场触发一次幂等键重放校验。
			if idx == 0 {
				reqDup, _ := http.NewRequest(http.MethodPost, *serverURL+"/api/runs", bytes.NewReader(bodyBytes))
				reqDup.Header.Set("Content-Type", "application/json")
				reqDup.Header.Set("Idempotency-Key", key)
				dResp, dErr := client.Do(reqDup)
				if dErr == nil {
					var dup struct {
						RunID            string `json:"run_id"`
						IdempotentReplay bool   `json:"idempotent_replay"`
					}
					_ = json.NewDecoder(dResp.Body).Decode(&dup)
					dResp.Body.Close()
					if dResp.StatusCode == http.StatusOK && dup.IdempotentReplay && dup.RunID == out.RunID {
						idemReplayOK.Store(true)
					}
				}
			}
		}(i)
	}
	submitWG.Wait()

	fmt.Printf("Submitted %d runs (%d tasks) in %s | IdempotencyReplay=%v\n",
		*runsCount, *runsCount*10*(*trials), time.Since(startTime).Round(time.Millisecond), idemReplayOK.Load())

	peak := base
	deadline := time.Now().Add(*timeout)
	completed := make(map[string]bool, len(runIDs))

	for len(completed) < len(runIDs) {
		if time.Now().After(deadline) {
			log.Fatalf("timed out waiting for runs completion (%d/%d completed)", len(completed), len(runIDs))
		}
		cur := sampleMetrics(client, *serverURL+"/metrics")
		if cur.Goroutines > peak.Goroutines {
			peak.Goroutines = cur.Goroutines
		}
		if cur.HeapMB > peak.HeapMB {
			peak.HeapMB = cur.HeapMB
		}

		for _, id := range runIDs {
			if completed[id] {
				continue
			}
			resp, err := client.Get(*serverURL + "/api/runs/" + id)
			if err != nil {
				continue
			}
			var detail struct {
				Run struct {
					Status string `json:"status"`
				} `json:"run"`
			}
			_ = json.NewDecoder(resp.Body).Decode(&detail)
			resp.Body.Close()
			if detail.Run.Status == "completed" {
				completed[id] = true
			}
		}
		if len(completed) < len(runIDs) {
			time.Sleep(500 * time.Millisecond)
		}
	}

	totalElapsed := time.Since(startTime)

	// 校验所有场次的汇总结果、重试恢复数与 HTML 战报可用性。
	var totalTasks, successTasks, failedTasks, retriedTasks, reportOK int
	for _, id := range runIDs {
		resp, err := client.Get(*serverURL + "/api/runs/" + id)
		if err != nil {
			log.Fatalf("GET run %s: %v", id, err)
		}
		var detail struct {
			Run struct {
				TotalTasks  int `json:"total_tasks"`
				DoneTasks   int `json:"done_tasks"`
				FailedTasks int `json:"failed_tasks"`
			} `json:"run"`
			Tasks []struct {
				Status     string `json:"status"`
				RetryCount int    `json:"retry_count"`
			} `json:"tasks"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&detail)
		resp.Body.Close()

		totalTasks += detail.Run.TotalTasks
		failedTasks += detail.Run.FailedTasks
		for _, tk := range detail.Tasks {
			if tk.Status == "success" {
				successTasks++
			}
			if tk.RetryCount > 0 {
				retriedTasks++
			}
		}

		rResp, rErr := client.Get(*serverURL + "/api/runs/" + id + "/report")
		if rErr == nil {
			_, _ = io.Copy(io.Discard, rResp.Body)
			rResp.Body.Close()
			if rResp.StatusCode == http.StatusOK {
				reportOK++
			}
		}
	}

	// 静置 10 秒后采集 Server 空闲状态与协程回落情况。
	time.Sleep(10 * time.Second)
	idle := sampleMetrics(client, *serverURL+"/metrics")

	fmt.Println("=== 分布式集群慢流式压测验收汇总 ===")
	fmt.Printf("TotalRuns=%d CompletedRuns=%d ReportsOK=%d IdempotencyReplay=%v\n",
		*runsCount, len(completed), reportOK, idemReplayOK.Load())
	fmt.Printf("TotalTasks=%d SuccessTasks=%d FailedTasks=%d RecoveredFrom429Retry=%d\n",
		totalTasks, successTasks, failedTasks, retriedTasks)
	fmt.Printf("Elapsed=%s EffectiveThroughput=%.2f tasks/s\n",
		totalElapsed.Round(time.Millisecond), float64(totalTasks)/totalElapsed.Seconds())
	fmt.Printf("Server Goroutines: Baseline=%d -> Peak=%d -> Idle(+10s)=%d\n",
		base.Goroutines, peak.Goroutines, idle.Goroutines)
	fmt.Printf("Server HeapAlloc : Baseline=%.2f MB -> Peak=%.2f MB -> Idle(+10s)=%.2f MB\n",
		base.HeapMB, peak.HeapMB, idle.HeapMB)
}

func sampleMetrics(client *http.Client, url string) runtimeSample {
	resp, err := client.Get(url)
	if err != nil {
		return runtimeSample{}
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)

	var s runtimeSample
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(line, "go_goroutines ") {
			v, _ := strconv.ParseFloat(strings.TrimSpace(strings.TrimPrefix(line, "go_goroutines ")), 64)
			s.Goroutines = int(v)
		} else if strings.HasPrefix(line, "go_memstats_alloc_bytes ") {
			v, _ := strconv.ParseFloat(strings.TrimSpace(strings.TrimPrefix(line, "go_memstats_alloc_bytes ")), 64)
			s.HeapMB = v / (1024 * 1024)
		}
	}
	return s
}
