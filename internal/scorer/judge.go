package scorer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"evalreel/internal/config"
	"evalreel/internal/runner"
	"evalreel/internal/store"
)

// JudgeScorer 使用强模型做二元（Pass/Fail）裁判，并记录评分标准版本号。
type JudgeScorer struct {
	name       string
	model      string
	baseURL    string
	apiKeyEnv  string
	rubric     string
	rubricHash string
	transport  *http.Transport
	client     *http.Client
}

// NewJudgeScorer 初始化二元 AI 裁判打分器。
func NewJudgeScorer(cfg config.ScorerConfig) (*JudgeScorer, error) {
	if cfg.JudgeBaseURL == "" || cfg.JudgeModel == "" {
		return nil, fmt.Errorf("judge scorer requires judge_base_url and judge_model")
	}
	name := cfg.Name
	if name == "" {
		name = "judge"
	}
	rubric := cfg.Rubric
	if rubric == "" {
		rubric = "判断模型输出是否准确、完整且直接回答了题目要求。"
	}
	sum := sha256.Sum256([]byte(cfg.JudgeModel + "\n" + rubric))

	tr := &http.Transport{
		DisableCompression: true,
		MaxIdleConns:       20,
		IdleConnTimeout:    30 * time.Second,
	}
	return &JudgeScorer{
		name:       name,
		model:      cfg.JudgeModel,
		baseURL:    strings.TrimRight(cfg.JudgeBaseURL, "/"),
		apiKeyEnv:  cfg.JudgeAPIKeyEnv,
		rubric:     rubric,
		rubricHash: hex.EncodeToString(sum[:8]),
		transport:  tr,
		client:     &http.Client{Transport: tr, Timeout: 60 * time.Second},
	}, nil
}

// Name 返回批卷器名称。
func (j *JudgeScorer) Name() string {
	return j.name
}

// CloseIdleConnections 清理裁判客户端残留的空闲连接。
func (j *JudgeScorer) CloseIdleConnections() {
	if j.transport != nil {
		j.transport.CloseIdleConnections()
	}
}

// Score 调用裁判模型输出结构化 JSON，按二元布尔值判分。
func (j *JudgeScorer) Score(ctx context.Context, item runner.DatasetItem, out runner.RunOutput) (store.EvalScore, error) {
	score := store.EvalScore{
		ScorerName: j.name,
		RubricHash: j.rubricHash,
	}

	prompt := fmt.Sprintf(
		"你是严格的自动化评测裁判。请先逐项核查以下评分标准，再给出最终布尔判定。\n"+
			"评分标准：%s\n\n"+
			"题目输入：%s%s\n"+
			"被测输出：%s\n\n"+
			"请只输出严格合法的 JSON，不要输出其他多余文字，格式：\n"+
			"{\"checklist\":[{\"item\":\"核查点\",\"ok\":true}],\"reason\":\"简明原因\",\"passed\":true}",
		j.rubric, item.Prompt, string(item.Messages), out.ResponseText,
	)

	reqPayload, _ := json.Marshal(map[string]any{
		"model":       j.model,
		"stream":      false,
		"temperature": 0,
		"messages": []map[string]string{
			{"role": "user", "content": prompt},
		},
	})

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, j.baseURL+"/chat/completions", bytes.NewReader(reqPayload))
	if err != nil {
		return score, err
	}
	req.Header.Set("Content-Type", "application/json")
	if j.apiKeyEnv != "" {
		if k := os.Getenv(j.apiKeyEnv); k != "" {
			req.Header.Set("Authorization", "Bearer "+k)
		}
	}

	resp, err := j.client.Do(req)
	if err != nil {
		return score, fmt.Errorf("judge request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 2*1024*1024))
	if err != nil {
		return score, err
	}
	if resp.StatusCode >= 400 {
		return score, fmt.Errorf("judge http %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var chatResp struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(body, &chatResp); err != nil || len(chatResp.Choices) == 0 {
		return score, fmt.Errorf("invalid judge response envelope")
	}

	rawJSON := extractJSONObject(chatResp.Choices[0].Message.Content)
	var verdict struct {
		Passed    bool   `json:"passed"`
		Reason    string `json:"reason"`
		Checklist any    `json:"checklist"`
	}
	if err := json.Unmarshal([]byte(rawJSON), &verdict); err != nil {
		score.Passed = false
		score.Reason = "judge returned invalid json: " + err.Error()
		score.DetailsJSON = rawJSON
		return score, nil
	}

	score.Passed = verdict.Passed
	score.Reason = verdict.Reason
	score.DetailsJSON = rawJSON
	return score, nil
}

func extractJSONObject(raw string) string {
	s := strings.TrimSpace(raw)
	if strings.HasPrefix(s, "```") {
		if nl := strings.IndexByte(s, '\n'); nl >= 0 {
			s = s[nl+1:]
		}
		if end := strings.LastIndex(s, "```"); end >= 0 {
			s = s[:end]
		}
		s = strings.TrimSpace(s)
	}
	start := strings.IndexByte(s, '{')
	end := strings.LastIndexByte(s, '}')
	if start >= 0 && end > start {
		return s[start : end+1]
	}
	return s
}
