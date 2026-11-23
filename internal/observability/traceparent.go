package observability

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"go.opentelemetry.io/otel/trace"
)

// TraceContext 保存从 W3C traceparent 请求头解析出的链路编号、上级节点编号和采样标志。
type TraceContext struct {
	TraceID string
	SpanID  string
	Flags   string
}

// ParseTraceparent 解析并校验标准 W3C traceparent 请求头（格式：00-<32位hex>-<16位hex>-<2位hex>）。
func ParseTraceparent(header string) (TraceContext, bool) {
	raw := strings.ToLower(strings.TrimSpace(header))
	if raw == "" {
		return TraceContext{}, false
	}
	parts := strings.Split(raw, "-")
	if len(parts) != 4 {
		return TraceContext{}, false
	}
	version, traceID, spanID, flags := parts[0], parts[1], parts[2], parts[3]
	if version != "00" {
		return TraceContext{}, false
	}
	if !isHexOfLen(traceID, 32) || isAllZero(traceID) {
		return TraceContext{}, false
	}
	if !isHexOfLen(spanID, 16) || isAllZero(spanID) {
		return TraceContext{}, false
	}
	if !isHexOfLen(flags, 2) {
		return TraceContext{}, false
	}
	return TraceContext{
		TraceID: traceID,
		SpanID:  spanID,
		Flags:   flags,
	}, true
}

// FormatTraceparent 组装标准 W3C traceparent 请求头字符串，默认开启采样标志 01。
func FormatTraceparent(traceID, spanID string) string {
	return fmt.Sprintf("00-%s-%s-01", strings.ToLower(traceID), strings.ToLower(spanID))
}

// NewTraceID 生成 128 位（32 个小写十六进制字符）随机链路编号。
func NewTraceID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	// 确保不会全为 0，符合 W3C 规范。
	b[0] |= 0x01
	return hex.EncodeToString(b[:])
}

// NewSpanID 生成 64 位（16 个小写十六进制字符）随机节点编号。
func NewSpanID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	b[0] |= 0x01
	return hex.EncodeToString(b[:])
}

// ContextWithParent 将解析出的上游 TraceContext 注入 Go context，使下游生成的 OTel Span 自动继承父子关系。
func ContextWithParent(ctx context.Context, tc TraceContext) context.Context {
	tid, err := trace.TraceIDFromHex(tc.TraceID)
	if err != nil {
		return ctx
	}
	sid, err := trace.SpanIDFromHex(tc.SpanID)
	if err != nil {
		return ctx
	}
	var traceFlags trace.TraceFlags
	if v, pErr := strconv.ParseUint(tc.Flags, 16, 8); pErr == nil {
		traceFlags = trace.TraceFlags(v)
	} else {
		traceFlags = trace.FlagsSampled
	}
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    tid,
		SpanID:     sid,
		TraceFlags: traceFlags,
		Remote:     true,
	})
	return trace.ContextWithRemoteSpanContext(ctx, sc)
}

func isHexOfLen(s string, wantLen int) bool {
	if len(s) != wantLen {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		isDigit := c >= '0' && c <= '9'
		isHex := c >= 'a' && c <= 'f'
		if !isDigit && !isHex {
			return false
		}
	}
	return true
}

func isAllZero(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] != '0' {
			return false
		}
	}
	return true
}
