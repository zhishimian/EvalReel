package scorer

import (
	"context"
	"fmt"

	"evalreel/internal/config"
	"evalreel/internal/runner"
	"evalreel/internal/store"
)

// Scorer 定义自动批卷器的统一接口，输入题目与执行输出，返回单条得分记录。
type Scorer interface {
	Name() string
	Score(ctx context.Context, item runner.DatasetItem, out runner.RunOutput) (store.EvalScore, error)
}

// BuildScorers 根据 JSON 配置初始化组合式批卷器列表。
func BuildScorers(cfgs []config.ScorerConfig) ([]Scorer, error) {
	scorers := make([]Scorer, 0, len(cfgs))
	for i, c := range cfgs {
		switch c.Type {
		case "rule":
			s, err := NewRuleScorer(c)
			if err != nil {
				return nil, fmt.Errorf("scorer[%d]: %w", i, err)
			}
			scorers = append(scorers, s)
		case "judge":
			s, err := NewJudgeScorer(c)
			if err != nil {
				return nil, fmt.Errorf("scorer[%d]: %w", i, err)
			}
			scorers = append(scorers, s)
		case "exec":
			scorers = append(scorers, NewExecScorer(c))
		default:
			return nil, fmt.Errorf("scorer[%d]: unsupported type %q", i, c.Type)
		}
	}
	return scorers, nil
}

// CloseScorers 释放批卷器内部持有的空闲 HTTP 连接。
func CloseScorers(scorers []Scorer) {
	for _, s := range scorers {
		if j, ok := s.(*JudgeScorer); ok {
			j.CloseIdleConnections()
		}
	}
}
