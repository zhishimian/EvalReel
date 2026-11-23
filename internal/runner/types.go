package runner

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"evalreel/internal/config"
	"evalreel/internal/model"
)

// DatasetItem 表示从 JSONL 题库或历史轨迹中读取出的单道待测题目。
type DatasetItem struct {
	ID         string          `json:"id"`
	Prompt     string          `json:"prompt,omitempty"`
	EntryPoint string          `json:"entry_point,omitempty"`
	Test       string          `json:"test,omitempty"`
	Messages   json.RawMessage `json:"messages,omitempty"`
	Tools      json.RawMessage `json:"tools,omitempty"`
	Expected   json.RawMessage `json:"expected,omitempty"`
}

// RunOutput 保存单次执行器跑完一道题后产出的文本、工具调用和消耗指标。
type RunOutput struct {
	TraceID       string
	ReasoningText string
	ResponseText  string
	ToolCalls     []model.ToolCall
	FinishReason  string
	TTFTMs        int64
	TotalMs       int64
	InputTokens   int
	OutputTokens  int
	CostUSD       float64
	WorkDir       string // 隔离子进程模式下供后续批卷检查的临时工作目录
}

// Runner 定义任务执行器的统一接口，覆盖纯大模型流式、读档分叉和本地子进程 Agent。
type Runner interface {
	Execute(ctx context.Context, target config.TargetConfig, item DatasetItem) (RunOutput, error)
}

// LoadDataset 从 JSONL 文件逐行读取全部题目。
func LoadDataset(path string) ([]DatasetItem, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open dataset %s: %w", path, err)
	}
	defer f.Close()

	var items []DatasetItem
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), 4*1024*1024)

	lineNo := 0
	for sc.Scan() {
		lineNo++
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var item DatasetItem
		if err := json.Unmarshal([]byte(line), &item); err != nil {
			return nil, fmt.Errorf("parse dataset line %d: %w", lineNo, err)
		}
		if item.ID == "" {
			item.ID = fmt.Sprintf("item-%03d", len(items)+1)
		}
		items = append(items, item)
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("read dataset %s: %w", path, err)
	}
	return items, nil
}
