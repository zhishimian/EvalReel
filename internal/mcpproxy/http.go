package mcpproxy

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// HTTPProxy 实现兼容 MCP Streamable HTTP（JSON 与 SSE 响应）的反向代理处理器。
type HTTPProxy struct {
	interceptor *Interceptor
	upstreamURL string
	client      *http.Client
}

// NewHTTPProxy 创建基于 HTTP 传输的 MCP 透明代理；严格 Mock 模式下 upstreamURL 可为空。
func NewHTTPProxy(interceptor *Interceptor, upstreamURL string, timeout time.Duration) *HTTPProxy {
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	return &HTTPProxy{
		interceptor: interceptor,
		upstreamURL: strings.TrimSpace(upstreamURL),
		client:      &http.Client{Timeout: timeout},
	}
}

// CloseIdleConnections 释放代理内部维持的上游 HTTP 空闲连接。
func (p *HTTPProxy) CloseIdleConnections() {
	if p.client != nil {
		p.client.CloseIdleConnections()
	}
}

// ServeHTTP 接收客户端 POST 请求，若属于 tools/call 则走拦截器，其余报文原样转发。
func (p *HTTPProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	reqBody, err := io.ReadAll(io.LimitReader(r.Body, 4*1024*1024))
	_ = r.Body.Close()
	if err != nil {
		http.Error(w, "read request failed", http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	if tp := strings.TrimSpace(r.Header.Get("traceparent")); tp != "" {
		ctx = WithTraceparent(ctx, tp)
	}

	var msg JSONRPCMessage
	if err := json.Unmarshal(reqBody, &msg); err != nil {
		out, _ := BuildErrorResponse(nil, -32700, "invalid json-rpc body")
		writeJSONRPC(w, http.StatusBadRequest, out)
		return
	}

	forward := func(fCtx context.Context, payload []byte) ([]byte, error) {
		if p.upstreamURL == "" {
			return nil, fmt.Errorf("upstream mcp url is empty")
		}
		upReq, err := http.NewRequestWithContext(fCtx, http.MethodPost, p.upstreamURL, bytes.NewReader(payload))
		if err != nil {
			return nil, err
		}
		for k, vals := range r.Header {
			if strings.EqualFold(k, "Host") || strings.EqualFold(k, "Content-Length") {
				continue
			}
			for _, v := range vals {
				upReq.Header.Add(k, v)
			}
		}
		if upReq.Header.Get("Content-Type") == "" {
			upReq.Header.Set("Content-Type", "application/json")
		}

		resp, err := p.client.Do(upReq)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()

		ct := strings.ToLower(resp.Header.Get("Content-Type"))
		if strings.Contains(ct, "text/event-stream") {
			return extractFirstSSEJSON(resp.Body)
		}
		raw, rErr := io.ReadAll(io.LimitReader(resp.Body, 8*1024*1024))
		if rErr != nil {
			return nil, rErr
		}
		if resp.StatusCode >= 400 && !json.Valid(raw) {
			return nil, fmt.Errorf("upstream http %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
		}
		return raw, nil
	}

	if _, isTool := ParseToolCall(msg); isTool {
		out, spanID, hErr := p.interceptor.HandleToolCall(ctx, msg, reqBody, forward)
		if spanID != "" {
			w.Header().Set("X-EvalReel-Span-ID", spanID)
		}
		if hErr != nil {
			errOut, _ := BuildErrorResponse(msg.ID, -32603, hErr.Error())
			writeJSONRPC(w, http.StatusBadGateway, errOut)
			return
		}
		writeJSONRPC(w, http.StatusOK, out)
		return
	}

	raw, fErr := forward(ctx, reqBody)
	if fErr != nil {
		errOut, _ := BuildErrorResponse(msg.ID, -32603, fErr.Error())
		writeJSONRPC(w, http.StatusBadGateway, errOut)
		return
	}
	writeJSONRPC(w, http.StatusOK, raw)
}

func extractFirstSSEJSON(r io.Reader) ([]byte, error) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 32*1024), 4*1024*1024)
	var lastData []byte
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if payload == "" || payload == "[DONE]" {
			continue
		}
		lastData = []byte(payload)
		var msg JSONRPCMessage
		if err := json.Unmarshal(lastData, &msg); err == nil && (len(msg.Result) > 0 || msg.Error != nil) {
			return lastData, nil
		}
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	if len(lastData) > 0 {
		return lastData, nil
	}
	return nil, fmt.Errorf("empty sse response from upstream mcp server")
}

func writeJSONRPC(w http.ResponseWriter, status int, body []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}
