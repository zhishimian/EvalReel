package mcpproxy

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// MCPRecord 是写入 .mcp.jsonl.gz 磁带文件中的单条 MCP 工具调用记录。
type MCPRecord struct {
	Timestamp      time.Time       `json:"timestamp"`
	TraceID        string          `json:"trace_id,omitempty"`
	SpanID         string          `json:"span_id,omitempty"`
	ParentSpanID   string          `json:"parent_span_id,omitempty"`
	Method         string          `json:"method"`
	ToolName       string          `json:"tool_name"`
	ArgsCanonical  string          `json:"args_canonical"`
	ArgsHash       string          `json:"args_hash"`
	RequestParams  json.RawMessage `json:"request_params,omitempty"`
	ResponseResult json.RawMessage `json:"response_result,omitempty"`
	Error          *JSONRPCError   `json:"error,omitempty"`
	IsToolError    bool            `json:"is_tool_error,omitempty"`
	DurationMs     int64           `json:"duration_ms"`
	ChaosRule      string          `json:"chaos_rule,omitempty"`
}

// TapeRecorder 将 MCP 调用过程并发安全地压成 gzip JSONL 磁带文件。
type TapeRecorder struct {
	mu     sync.Mutex
	file   *os.File
	gz     *gzip.Writer
	bw     *bufio.Writer
	closed bool
}

// NewTapeRecorder 创建并初始化 .mcp.jsonl.gz 录制文件（自动建立父目录）。
func NewTapeRecorder(path string) (*TapeRecorder, error) {
	dir := filepath.Dir(path)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("create tape dir %s: %w", dir, err)
		}
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open tape file %s: %w", path, err)
	}
	gz := gzip.NewWriter(f)
	bw := bufio.NewWriterSize(gz, 32*1024)
	return &TapeRecorder{file: f, gz: gz, bw: bw}, nil
}

// Write 追加写入一条工具调用记录并刷新至底层 gzip 流。
func (r *TapeRecorder) Write(rec MCPRecord) error {
	if r == nil {
		return nil
	}
	if rec.Timestamp.IsZero() {
		rec.Timestamp = time.Now().UTC()
	}
	b, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("marshal mcp record: %w", err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return fmt.Errorf("tape recorder already closed")
	}
	if _, err := r.bw.Write(b); err != nil {
		return err
	}
	if err := r.bw.WriteByte('\n'); err != nil {
		return err
	}
	return r.bw.Flush()
}

// Close 刷新压缩尾并安全关闭磁带文件。
func (r *TapeRecorder) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	var firstErr error
	if err := r.bw.Flush(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := r.gz.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := r.file.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

// TapeStore 保存离线 Mock 模式加载的工具录制磁带，支持按参数哈希依次命中多次相同调用。
type TapeStore struct {
	mu      sync.Mutex
	records map[string][]MCPRecord
	cursors map[string]int
}

// LoadTape 从 .mcp.jsonl.gz 文件加载所有已录制工具调用记录。
func LoadTape(path string) (*TapeStore, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open mock tape %s: %w", path, err)
	}
	defer f.Close()

	gz, err := gzip.NewReader(f)
	if err != nil {
		return nil, fmt.Errorf("open gzip tape %s: %w", path, err)
	}
	defer gz.Close()

	var recs []MCPRecord
	sc := bufio.NewScanner(gz)
	sc.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var rec MCPRecord
		if err := json.Unmarshal(line, &rec); err != nil {
			return nil, fmt.Errorf("parse tape record: %w", err)
		}
		recs = append(recs, rec)
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("scan tape %s: %w", path, err)
	}
	return NewTapeStore(recs), nil
}

// NewTapeStore 从内存记录切片构造线程安全的 Mock 磁带索引。
func NewTapeStore(recs []MCPRecord) *TapeStore {
	m := make(map[string][]MCPRecord, len(recs))
	for _, rec := range recs {
		if rec.ArgsHash == "" && rec.ToolName != "" {
			rec.ArgsHash = ToolCallKey(rec.ToolName, json.RawMessage(rec.ArgsCanonical))
		}
		m[rec.ArgsHash] = append(m[rec.ArgsHash], rec)
	}
	return &TapeStore{
		records: m,
		cursors: make(map[string]int, len(m)),
	}
}

// Match 按参数哈希查找录制的返回值；若同一入参被调用多次，按录制先后顺序逐条返回。
func (ts *TapeStore) Match(argsHash string) (MCPRecord, bool) {
	if ts == nil || argsHash == "" {
		return MCPRecord{}, false
	}
	ts.mu.Lock()
	defer ts.mu.Unlock()

	list := ts.records[argsHash]
	if len(list) == 0 {
		return MCPRecord{}, false
	}
	idx := ts.cursors[argsHash]
	if idx >= len(list) {
		idx = len(list) - 1
	} else {
		ts.cursors[argsHash] = idx + 1
	}
	return list[idx], true
}
