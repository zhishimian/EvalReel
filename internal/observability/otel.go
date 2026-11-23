package observability

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

// OTelConfig 配置 OpenTelemetry 链路导出方式与接收端地址。
type OTelConfig struct {
	ServiceName string
	Exporter    string // none | stdout | otlp
	Endpoint    string // otlp 模式下的收集端地址，例如 http://jaeger:4318
	StdoutOut   io.Writer
}

// GenAIAttrs 定义大模型调用链路节点开始时的通用语义属性。
type GenAIAttrs struct {
	System       string
	RequestModel string
	RunID        string
	TaskID       string
	AgentName    string
}

// GenAIResult 定义大模型调用结束时需要补记的响应模型、词元消耗与完成状态。
type GenAIResult struct {
	ResponseModel string
	InputTokens   int
	OutputTokens  int
	FinishReason  string
	Status        string
	Err           error
}

// InitTracer 初始化全局 OpenTelemetry TracerProvider 与 W3C 上下文传播器，并返回优雅关闭函数。
func InitTracer(ctx context.Context, cfg OTelConfig) (func(context.Context) error, error) {
	otel.SetTextMapPropagator(propagation.TraceContext{})

	mode := strings.ToLower(strings.TrimSpace(cfg.Exporter))
	if mode == "" || mode == "none" {
		otel.SetTracerProvider(tracenoop.NewTracerProvider())
		return func(context.Context) error { return nil }, nil
	}

	svcName := strings.TrimSpace(cfg.ServiceName)
	if svcName == "" {
		svcName = "evalreel"
	}
	res := resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName(svcName),
	)

	var opts []sdktrace.TracerProviderOption
	opts = append(opts, sdktrace.WithResource(res))

	switch mode {
	case "stdout":
		w := cfg.StdoutOut
		if w == nil {
			w = os.Stdout
		}
		exp, err := stdouttrace.New(stdouttrace.WithWriter(w))
		if err != nil {
			return nil, fmt.Errorf("create stdout trace exporter: %w", err)
		}
		opts = append(opts, sdktrace.WithSyncer(exp))
	case "otlp":
		var expOpts []otlptracehttp.Option
		ep := strings.TrimSpace(cfg.Endpoint)
		if ep != "" {
			if strings.HasPrefix(ep, "http://") || strings.HasPrefix(ep, "https://") {
				expOpts = append(expOpts, otlptracehttp.WithEndpointURL(ep))
			} else {
				expOpts = append(expOpts, otlptracehttp.WithEndpoint(ep), otlptracehttp.WithInsecure())
			}
		} else {
			expOpts = append(expOpts, otlptracehttp.WithInsecure())
		}
		exp, err := otlptracehttp.New(ctx, expOpts...)
		if err != nil {
			return nil, fmt.Errorf("create otlp http exporter: %w", err)
		}
		opts = append(opts, sdktrace.WithBatcher(exp))
	default:
		return nil, fmt.Errorf("unsupported otel exporter %q (want none|stdout|otlp)", cfg.Exporter)
	}

	tp := sdktrace.NewTracerProvider(opts...)
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}

// StartGenAISpan 开启一条带 OpenTelemetry GenAI 标准语义属性的链路节点。
func StartGenAISpan(ctx context.Context, name string, attrs GenAIAttrs) (context.Context, trace.Span) {
	system := attrs.System
	if system == "" {
		system = "openai"
	}
	kv := []attribute.KeyValue{
		attribute.String("gen_ai.system", system),
	}
	if attrs.RequestModel != "" {
		kv = append(kv, attribute.String("gen_ai.request.model", attrs.RequestModel))
	}
	if attrs.RunID != "" {
		kv = append(kv, attribute.String("evalreel.run_id", attrs.RunID))
	}
	if attrs.TaskID != "" {
		kv = append(kv, attribute.String("evalreel.task_id", attrs.TaskID))
	}
	if attrs.AgentName != "" {
		kv = append(kv, attribute.String("evalreel.agent_name", attrs.AgentName))
	}
	return otel.Tracer("evalreel").Start(ctx, name, trace.WithAttributes(kv...))
}

// EndGenAISpan 补齐模型响应与词元统计属性，按需记录错误状态并关闭 Span。
func EndGenAISpan(span trace.Span, res GenAIResult) {
	if span == nil {
		return
	}
	var kv []attribute.KeyValue
	if res.ResponseModel != "" {
		kv = append(kv, attribute.String("gen_ai.response.model", res.ResponseModel))
	}
	if res.InputTokens > 0 {
		kv = append(kv, attribute.Int("gen_ai.usage.input_tokens", res.InputTokens))
	}
	if res.OutputTokens > 0 {
		kv = append(kv, attribute.Int("gen_ai.usage.output_tokens", res.OutputTokens))
	}
	if res.FinishReason != "" {
		kv = append(kv, attribute.StringSlice("gen_ai.response.finish_reasons", []string{res.FinishReason}))
	}
	if res.Status != "" {
		kv = append(kv, attribute.String("evalreel.status", res.Status))
	}
	if len(kv) > 0 {
		span.SetAttributes(kv...)
	}
	if res.Err != nil {
		span.RecordError(res.Err)
		span.SetStatus(codes.Error, res.Err.Error())
	} else if res.Status == "error" || res.Status == "failed" {
		span.SetStatus(codes.Error, "operation failed")
	}
	span.End()
}
