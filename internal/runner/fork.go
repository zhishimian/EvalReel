package runner

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"evalreel/internal/model"
)

// LoadForkItem 读取 v1.0 录制的 .jsonl.gz 或 .jsonl 历史轨迹，抽取出第 N 步输入供横评对比。
func LoadForkItem(traceFile, traceID string, stepIndex int) (DatasetItem, error) {
	records, err := readTraceRecords(traceFile)
	if err != nil {
		return DatasetItem{}, err
	}
	if len(records) == 0 {
		return DatasetItem{}, fmt.Errorf("no trace records found in %s", traceFile)
	}

	var chosen *model.TraceRecord
	chosenIdx := stepIndex
	if traceID != "" {
		for i := range records {
			if records[i].TraceID == traceID {
				chosen = &records[i]
				chosenIdx = i
				break
			}
		}
		if chosen == nil {
			return DatasetItem{}, fmt.Errorf("trace_id %q not found in %s", traceID, traceFile)
		}
	} else {
		if stepIndex < 0 || stepIndex >= len(records) {
			return DatasetItem{}, fmt.Errorf("step_index %d out of range [0, %d)", stepIndex, len(records))
		}
		chosen = &records[stepIndex]
	}

	if len(chosen.RequestBody) == 0 {
		return DatasetItem{}, fmt.Errorf("trace %s has empty request_body", chosen.TraceID)
	}

	var reqBody struct {
		Messages json.RawMessage `json:"messages"`
		Tools    json.RawMessage `json:"tools"`
	}
	if err := json.Unmarshal(chosen.RequestBody, &reqBody); err != nil {
		return DatasetItem{}, fmt.Errorf("unmarshal request_body from %s: %w", chosen.TraceID, err)
	}
	if len(reqBody.Messages) == 0 {
		return DatasetItem{}, fmt.Errorf("trace %s request_body missing messages", chosen.TraceID)
	}

	return DatasetItem{
		ID:       fmt.Sprintf("fork_%s_step%d", chosen.TraceID, chosenIdx),
		Messages: reqBody.Messages,
		Tools:    reqBody.Tools,
	}, nil
}

func readTraceRecords(traceFile string) ([]model.TraceRecord, error) {
	f, err := os.Open(traceFile)
	if err != nil {
		return nil, fmt.Errorf("open trace file %s: %w", traceFile, err)
	}
	defer f.Close()

	var reader io.Reader = f
	if strings.HasSuffix(strings.ToLower(traceFile), ".gz") {
		gz, err := gzip.NewReader(f)
		if err != nil {
			return nil, fmt.Errorf("open gzip trace %s: %w", traceFile, err)
		}
		defer gz.Close()
		reader = gz
	}

	var records []model.TraceRecord
	br := bufio.NewReaderSize(reader, 256*1024)
	for {
		line, err := br.ReadString('\n')
		trimmed := strings.TrimSpace(line)
		if trimmed != "" {
			var rec model.TraceRecord
			if uErr := json.Unmarshal([]byte(trimmed), &rec); uErr == nil {
				records = append(records, rec)
			}
		}
		if err != nil {
			// 允许意外强杀留下的缺少文件尾的 .jsonl.gz 已刷新部分仍可读出
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return records, fmt.Errorf("read trace %s: %w", traceFile, err)
		}
	}
	return records, nil
}
