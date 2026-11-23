package recorder

import (
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"evalreel/internal/model"
)

// ErrRecorderClosed 表示记录器已经关闭，不能再继续写入轨迹。
var ErrRecorderClosed = errors.New("recorder is closed")

// Recorder 定义轨迹落盘接口，v1.0 使用本地压缩文件，v2.0 可无缝替换为数据库实现。
type Recorder interface {
	Write(rec model.TraceRecord) error
	Close() error
}

// GzipJSONLRecorder 将每条调用轨迹以一行 JSON 的格式安全追加写入 .jsonl.gz 压缩文件。
type GzipJSONLRecorder struct {
	mu     sync.Mutex
	path   string
	file   *os.File
	gz     *gzip.Writer
	closed bool
}

// NewGzipJSONLRecorder 创建输出目录和 .jsonl.gz 文件，并初始化压缩写入器。
func NewGzipJSONLRecorder(filePath string) (*GzipJSONLRecorder, error) {
	dir := filepath.Dir(filePath)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("create trace dir %s: %w", dir, err)
		}
	}

	f, err := os.Create(filePath)
	if err != nil {
		return nil, fmt.Errorf("create trace file %s: %w", filePath, err)
	}

	return &GzipJSONLRecorder{
		path: filePath,
		file: f,
		gz:   gzip.NewWriter(f),
	}, nil
}

// Path 返回当前记录器写入的磁盘文件路径。
func (r *GzipJSONLRecorder) Path() string {
	return r.path
}

// Write 将单条轨迹编码为单行 JSON，加锁写入压缩流并立即刷新缓冲区。
func (r *GzipJSONLRecorder) Write(rec model.TraceRecord) error {
	if rec.Timestamp.IsZero() {
		rec.Timestamp = time.Now().UTC()
	}

	line, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("marshal trace record: %w", err)
	}
	line = append(line, '\n')

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return ErrRecorderClosed
	}
	if _, err := r.gz.Write(line); err != nil {
		return fmt.Errorf("write gzip trace: %w", err)
	}
	if err := r.gz.Flush(); err != nil {
		return fmt.Errorf("flush gzip trace: %w", err)
	}
	return nil
}

// Close 安全写入 gzip 文件尾校验数据并关闭文件，支持重复调用不报错。
func (r *GzipJSONLRecorder) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil
	}
	r.closed = true

	gzErr := r.gz.Close()
	fileErr := r.file.Close()
	return errors.Join(gzErr, fileErr)
}
