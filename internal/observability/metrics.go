package observability

import (
	"net/http"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	defaultMetricsOnce sync.Once
	defaultMetricsInst *Metrics
)

// Metrics 封装独立的 Prometheus 指标注册表，避免多组件测试时污染全局 DefaultRegisterer。
type Metrics struct {
	reg                *prometheus.Registry
	proxyRequestsTotal *prometheus.CounterVec
	proxyActiveStreams prometheus.Gauge
	llmTTFTMs          *prometheus.HistogramVec
	workerTasksTotal   *prometheus.CounterVec
	workerTaskDurMs    *prometheus.HistogramVec
	limiterWaitMs      *prometheus.HistogramVec
}

// DefaultMetrics 返回进程内全局共用的 EvalReel 监控指标实例。
func DefaultMetrics() *Metrics {
	defaultMetricsOnce.Do(func() {
		defaultMetricsInst = NewMetrics()
	})
	return defaultMetricsInst
}

// NewMetrics 创建并注册所有 EvalReel 核心指标与 Go 运行时基础指标。
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	msBuckets := []float64{5, 25, 50, 100, 250, 500, 1000, 2500, 5000, 15000, 60000}

	m := &Metrics{
		reg: reg,
		proxyRequestsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "evalreel_proxy_requests_total",
			Help: "Total number of LLM requests handled by EvalReel transparent proxy.",
		}, []string{"model", "status", "chaos"}),
		proxyActiveStreams: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "evalreel_proxy_active_streams",
			Help: "Current number of active SSE streaming proxy connections.",
		}),
		llmTTFTMs: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "evalreel_llm_ttft_ms",
			Help:    "Time to first token latency in milliseconds.",
			Buckets: msBuckets,
		}, []string{"source", "model", "phase"}),
		workerTasksTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "evalreel_worker_tasks_total",
			Help: "Total evaluation sub-tasks processed by workers.",
		}, []string{"target", "status"}),
		workerTaskDurMs: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "evalreel_worker_task_duration_ms",
			Help:    "Duration of evaluation sub-tasks executed by workers in milliseconds.",
			Buckets: msBuckets,
		}, []string{"target"}),
		limiterWaitMs: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "evalreel_limiter_wait_ms",
			Help:    "Time spent waiting for distributed rate limiter quota in milliseconds.",
			Buckets: msBuckets,
		}, []string{"target"}),
	}

	reg.MustRegister(
		m.proxyRequestsTotal,
		m.proxyActiveStreams,
		m.llmTTFTMs,
		m.workerTasksTotal,
		m.workerTaskDurMs,
		m.limiterWaitMs,
	)
	return m
}

// Handler 返回标准 http.Handler 供 Gin 或 Go HTTP 服务挂载为 /metrics 端点。
func (m *Metrics) Handler() http.Handler {
	if m == nil {
		return http.NotFoundHandler()
	}
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}

// RecordProxyRequest 记录一次透明代理转发请求的最终状态与混沌命中情况。
func (m *Metrics) RecordProxyRequest(model, status, chaosRule string) {
	if m == nil {
		return
	}
	if model == "" {
		model = "unknown"
	}
	if chaosRule == "" {
		chaosRule = "none"
	}
	m.proxyRequestsTotal.WithLabelValues(model, status, chaosRule).Inc()
}

// IncActiveStreams 在流式响应开始透传时增加活跃连接计数。
func (m *Metrics) IncActiveStreams() {
	if m == nil {
		return
	}
	m.proxyActiveStreams.Inc()
}

// DecActiveStreams 在流式响应读取完成或断连回收时减少活跃连接计数。
func (m *Metrics) DecActiveStreams() {
	if m == nil {
		return
	}
	m.proxyActiveStreams.Dec()
}

// ObserveTTFT 记录单次调用的首字延迟（毫秒，phase 区分 reasoning 或 answer）。
func (m *Metrics) ObserveTTFT(source, model, phase string, ms int64) {
	if m == nil || ms <= 0 {
		return
	}
	if model == "" {
		model = "unknown"
	}
	m.llmTTFTMs.WithLabelValues(source, model, phase).Observe(float64(ms))
}

// RecordWorkerTask 记录 Worker 处理完一条子任务的最终结果与执行耗时（毫秒）。
func (m *Metrics) RecordWorkerTask(target, status string, durationMs int64) {
	if m == nil {
		return
	}
	if target == "" {
		target = "unknown"
	}
	m.workerTasksTotal.WithLabelValues(target, status).Inc()
	if durationMs >= 0 {
		m.workerTaskDurMs.WithLabelValues(target).Observe(float64(durationMs))
	}
}

// ObserveLimiterWait 记录请求被限流器阻塞等待配额放行的毫秒时长。
func (m *Metrics) ObserveLimiterWait(target string, waitMs int64) {
	if m == nil || waitMs < 0 {
		return
	}
	if target == "" {
		target = "unknown"
	}
	m.limiterWaitMs.WithLabelValues(target).Observe(float64(waitMs))
}
