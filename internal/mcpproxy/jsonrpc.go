package mcpproxy

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

// JSONRPCError 表示标准 JSON-RPC 2.0 协议错误对象。
type JSONRPCError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

// JSONRPCMessage 表示 MCP 协议中传输的单条请求、通知或响应报文。
type JSONRPCMessage struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *JSONRPCError   `json:"error,omitempty"`
}

// ToolCallParams 对应 MCP tools/call 方法的入参载荷。
type ToolCallParams struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments,omitempty"`
}

// ParseToolCall 判断报文是否为 MCP 工具调用请求并提取工具名和参数。
func ParseToolCall(msg JSONRPCMessage) (ToolCallParams, bool) {
	if msg.Method != "tools/call" || len(msg.Params) == 0 {
		return ToolCallParams{}, false
	}
	var p ToolCallParams
	if err := json.Unmarshal(msg.Params, &p); err != nil || p.Name == "" {
		return ToolCallParams{}, false
	}
	return p, true
}

// CanonicalJSON 对参数 JSON 按键名递归排序并输出紧凑格式，消除 Go/Python 字典键序差异。
func CanonicalJSON(raw json.RawMessage) string {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return "{}"
	}
	dec := json.NewDecoder(bytes.NewReader(trimmed))
	dec.UseNumber()
	var val any
	if err := dec.Decode(&val); err != nil {
		return string(trimmed)
	}
	b, err := json.Marshal(val)
	if err != nil {
		return string(trimmed)
	}
	return string(b)
}

// ToolCallKey 计算“工具名 + 规范化参数 JSON”的 SHA-256 哈希，用作离线 Mock 精确匹配键。
func ToolCallKey(toolName string, args json.RawMessage) string {
	canon := CanonicalJSON(args)
	sum := sha256.Sum256([]byte(toolName + "|" + canon))
	return hex.EncodeToString(sum[:])
}

// BuildResultResponse 构造标准 JSON-RPC 2.0 成功响应字节流。
func BuildResultResponse(id, result json.RawMessage) ([]byte, error) {
	if len(bytes.TrimSpace(result)) == 0 {
		result = json.RawMessage(`{}`)
	}
	msg := JSONRPCMessage{
		JSONRPC: "2.0",
		ID:      normalizeID(id),
		Result:  result,
	}
	return json.Marshal(msg)
}

// BuildErrorResponse 构造标准 JSON-RPC 2.0 协议层错误响应。
func BuildErrorResponse(id json.RawMessage, code int, errMsg string) ([]byte, error) {
	msg := JSONRPCMessage{
		JSONRPC: "2.0",
		ID:      normalizeID(id),
		Error: &JSONRPCError{
			Code:    code,
			Message: errMsg,
		},
	}
	return json.Marshal(msg)
}

// BuildToolErrorResponse 构造协议层成功、但 tool_is_error 为 true 的 MCP 工具执行失败响应，用于检验模型纠偏能力。
func BuildToolErrorResponse(id json.RawMessage, errText string) ([]byte, error) {
	if strings.TrimSpace(errText) == "" {
		errText = "injected mcp tool execution error"
	}
	payload, err := json.Marshal(map[string]any{
		"content": []map[string]any{
			{"type": "text", "text": errText},
		},
		"isError": true,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal tool error payload: %w", err)
	}
	return BuildResultResponse(id, payload)
}

func normalizeID(id json.RawMessage) json.RawMessage {
	if len(bytes.TrimSpace(id)) == 0 {
		return json.RawMessage(`null`)
	}
	return id
}
