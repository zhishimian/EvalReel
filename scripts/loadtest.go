//go:build ignore

package main

import (
	"bufio"
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type pprofSnapshot struct {
	TotalGoroutines int
	EvalReelStacks  int
	HeapAllocBytes  int64
	HeapInuseBytes  int64
}

func main() {
	proxyAddr := flag.String("proxy", "http://127.0.0.1:8080", "EvalReel 代理地址")
	pprofAddr := flag.String("pprof", "http://127.0.0.1:6060", "pprof 监控地址")
	concurrency := flag.Int("concurrency", 60, "并发协程数")
	duration := flag.Duration("duration", 2*time.Minute, "压测持续时间")
	cancelRate := flag.Float64("cancel-rate", 0.20, "客户端中途主动断开的比例")
	flag.Parse()

	pprofClient := &http.Client{
		Timeout:   5 * time.Second,
		Transport: &http.Transport{DisableKeepAlives: true},
	}

	baseline, err := samplePProf(pprofClient, *pprofAddr)
	if err != nil {
		fmt.Printf("采样基线失败: %v\n", err)
		return
	}
	fmt.Printf("[基线 Baseline] %s\n", formatSnap(baseline))

	loadTransport := &http.Transport{
		MaxIdleConns:        200,
		MaxIdleConnsPerHost: 200,
		IdleConnTimeout:     30 * time.Second,
	}
	loadClient := &http.Client{
		Transport: loadTransport,
		Timeout:   30 * time.Second,
	}

	var (
		totalReqs   atomic.Int64
		okReqs      atomic.Int64
		code429Reqs atomic.Int64
		canceledReq atomic.Int64
		errReqs     atomic.Int64
	)

	ctx, cancelAll := context.WithTimeout(context.Background(), *duration)
	defer cancelAll()

	var peakSnap pprofSnapshot
	var peakOnce sync.Once
	go func() {
		timer := time.NewTimer(*duration / 2)
		defer timer.Stop()
		select {
		case <-timer.C:
			peakOnce.Do(func() {
				if snap, err := samplePProf(pprofClient, *pprofAddr); err == nil {
					peakSnap = snap
					fmt.Printf("[峰值 Peak]     %s\n", formatSnap(peakSnap))
				}
			})
		case <-ctx.Done():
		}
	}()

	payload := []byte(`{"model":"mock-reasoning-v1","stream":true,"stream_options":{"include_usage":true},"messages":[{"role":"user","content":"loadtest"}]}`)

	var wg sync.WaitGroup
	wg.Add(*concurrency)
	for i := 0; i < *concurrency; i++ {
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				totalReqs.Add(1)
				shouldCancel := rand.Float64() < *cancelRate

				reqCtx, reqCancel := context.WithCancel(ctx)
				req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, strings.TrimRight(*proxyAddr, "/")+"/v1/chat/completions", bytes.NewReader(payload))
				if err != nil {
					reqCancel()
					errReqs.Add(1)
					continue
				}
				req.Header.Set("Content-Type", "application/json")

				resp, err := loadClient.Do(req)
				if err != nil {
					reqCancel()
					if ctx.Err() != nil {
						break
					}
					errReqs.Add(1)
					continue
				}

				if resp.StatusCode == http.StatusTooManyRequests {
					_, _ = io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
					reqCancel()
					code429Reqs.Add(1)
					continue
				}

				if resp.StatusCode != http.StatusOK {
					_, _ = io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
					reqCancel()
					errReqs.Add(1)
					continue
				}

				sc := bufio.NewScanner(resp.Body)
				sc.Buffer(make([]byte, 32*1024), 2*1024*1024)
				chunks := 0
				aborted := false
				for sc.Scan() {
					line := sc.Text()
					if strings.HasPrefix(line, "data: ") {
						chunks++
						if shouldCancel && chunks >= 2 {
							reqCancel()
							aborted = true
							break
						}
					}
				}
				scanErr := sc.Err()
				resp.Body.Close()
				reqCancel()

				if aborted {
					canceledReq.Add(1)
				} else if scanErr != nil {
					errReqs.Add(1)
				} else {
					okReqs.Add(1)
				}
			}
		}()
	}

	wg.Wait()
	loadTransport.CloseIdleConnections()

	fmt.Println("压测请求结束，等待 10 秒观察协程与内存回落...")
	time.Sleep(10 * time.Second)

	after, err := samplePProf(pprofClient, *pprofAddr)
	if err != nil {
		fmt.Printf("采样结束快照失败: %v\n", err)
		return
	}
	fmt.Printf("[结束 +10s]     %s\n", formatSnap(after))
	fmt.Printf("\n=== 请求统计 ===\n总请求=%d | 正常完成=%d | 429限流=%d | 主动断连=%d | 断流或错误=%d\n",
		totalReqs.Load(), okReqs.Load(), code429Reqs.Load(), canceledReq.Load(), errReqs.Load())
}

var (
	reTotalG = regexp.MustCompile(`goroutine profile: total (\d+)`)
	reHeapA  = regexp.MustCompile(`# HeapAlloc = (\d+)`)
	reHeapI  = regexp.MustCompile(`# HeapInuse = (\d+)`)
)

func samplePProf(client *http.Client, pprofAddr string) (pprofSnapshot, error) {
	var snap pprofSnapshot
	base := strings.TrimRight(pprofAddr, "/")

	gResp, err := client.Get(base + "/debug/pprof/goroutine?debug=1")
	if err != nil {
		return snap, err
	}
	gBytes, _ := io.ReadAll(gResp.Body)
	gResp.Body.Close()
	gText := string(gBytes)
	if m := reTotalG.FindStringSubmatch(gText); len(m) == 2 {
		snap.TotalGoroutines, _ = strconv.Atoi(m[1])
	}
	for _, line := range strings.Split(gText, "\n") {
		if strings.Contains(line, "evalreel/internal/") {
			snap.EvalReelStacks++
		}
	}

	hResp, err := client.Get(base + "/debug/pprof/heap?debug=1&gc=1")
	if err != nil {
		return snap, err
	}
	hBytes, _ := io.ReadAll(hResp.Body)
	hResp.Body.Close()
	hText := string(hBytes)
	if m := reHeapA.FindStringSubmatch(hText); len(m) == 2 {
		snap.HeapAllocBytes, _ = strconv.ParseInt(m[1], 10, 64)
	}
	if m := reHeapI.FindStringSubmatch(hText); len(m) == 2 {
		snap.HeapInuseBytes, _ = strconv.ParseInt(m[1], 10, 64)
	}
	return snap, nil
}

func formatSnap(s pprofSnapshot) string {
	return fmt.Sprintf("总协程=%d (业务残留栈=%d) | HeapAlloc=%.2f MB | HeapInuse=%.2f MB",
		s.TotalGoroutines,
		s.EvalReelStacks,
		float64(s.HeapAllocBytes)/(1024*1024),
		float64(s.HeapInuseBytes)/(1024*1024),
	)
}
