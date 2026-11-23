package mcpproxy

import (
	"math/rand/v2"
	"sync"
)

// ChaosRule 描述单条针对 MCP 工具调用的故障注入规则。
type ChaosRule struct {
	Name         string  `json:"name" yaml:"name"`
	ToolName     string  `json:"tool_name" yaml:"tool_name"` // 精确工具名或 "*" 匹配所有工具
	EveryN       int     `json:"every_n" yaml:"every_n"`
	Rate         float64 `json:"rate" yaml:"rate"`
	Action       string  `json:"action" yaml:"action"` // delay | timeout | jsonrpc_error | tool_is_error | malformed_json
	DelayMs      int     `json:"delay_ms" yaml:"delay_ms"`
	ErrorCode    int     `json:"error_code" yaml:"error_code"`
	ErrorMessage string  `json:"error_message" yaml:"error_message"`
}

// ChaosEngine 在工具侧并发安全地判定当前调用是否触发指定故障规则。
type ChaosEngine struct {
	mu       sync.Mutex
	rules    []ChaosRule
	counters map[int]int
}

// NewChaosEngine 根据规则切片构建 MCP 工具混沌引擎。
func NewChaosEngine(rules []ChaosRule) *ChaosEngine {
	copied := append([]ChaosRule(nil), rules...)
	return &ChaosEngine{
		rules:    copied,
		counters: make(map[int]int, len(copied)),
	}
}

// Evaluate 检查工具名是否命中规则；命中则返回对应规则快照与 true。
func (e *ChaosEngine) Evaluate(toolName string) (ChaosRule, bool) {
	if e == nil || len(e.rules) == 0 {
		return ChaosRule{}, false
	}
	e.mu.Lock()
	defer e.mu.Unlock()

	for i, rule := range e.rules {
		if rule.ToolName != "" && rule.ToolName != "*" && rule.ToolName != toolName {
			continue
		}
		if rule.EveryN > 0 {
			e.counters[i]++
			if e.counters[i]%rule.EveryN == 0 {
				return rule, true
			}
			continue
		}
		if rule.Rate > 0 && rand.Float64() < rule.Rate {
			return rule, true
		}
	}
	return ChaosRule{}, false
}
