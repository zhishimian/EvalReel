package chaos

import (
	"context"
	"encoding/json"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"evalreel/internal/config"
)

type compiledRule struct {
	rule    config.ChaosRule
	counter *atomic.Int64
}

// Engine 管理请求前（延迟/429/500）与流式中（断流/畸形 JSON）两类混沌注入规则。
type Engine struct {
	enabled     bool
	preRules    []compiledRule
	streamRules []compiledRule
	randFloat   func() float64
}

// NewEngine 根据配置构建混沌故障注入引擎。
func NewEngine(cfg config.ChaosConfig) *Engine {
	e := &Engine{
		enabled:   cfg.Enabled,
		randFloat: rand.Float64,
	}
	for _, r := range cfg.Rules {
		cr := compiledRule{
			rule:    r,
			counter: &atomic.Int64{},
		}
		switch r.Action {
		case "cut_stream", "malformed_json":
			e.streamRules = append(e.streamRules, cr)
		case "delay", "status_429", "status_500":
			e.preRules = append(e.preRules, cr)
		}
	}
	return e
}

// PreForward 返回请求前故障注入中间件。若命中 429/500 则直接回复客户端并通知回调；若命中 delay 则休眠后放行。
func (e *Engine) PreForward(
	next http.Handler,
	onDelay func(r *http.Request, rule config.ChaosRule) *http.Request,
	onIntercepted func(r *http.Request, rule config.ChaosRule, statusCode int),
) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if e == nil || !e.enabled {
			next.ServeHTTP(w, r)
			return
		}

		matched := e.matchRules(e.preRules, r.URL.Path)
		if matched == nil {
			next.ServeHTTP(w, r)
			return
		}

		switch matched.Action {
		case "delay":
			if matched.DelayMs > 0 {
				if !sleepContext(r.Context(), time.Duration(matched.DelayMs)*time.Millisecond) {
					if onIntercepted != nil {
						onIntercepted(r, *matched, 499)
					}
					return
				}
			}
			if onDelay != nil {
				r = onDelay(r, *matched)
			}
			next.ServeHTTP(w, r)

		case "status_429":
			retryAfter := matched.RetryAfterSec
			if retryAfter <= 0 {
				retryAfter = 2
			}
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Retry-After", strconv.Itoa(retryAfter))
			w.WriteHeader(http.StatusTooManyRequests)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"error": map[string]any{
					"message": "evalreel chaos injected: rate limit exceeded",
					"type":    "rate_limit_exceeded",
					"rule":    matched.Name,
				},
			})
			if onIntercepted != nil {
				onIntercepted(r, *matched, http.StatusTooManyRequests)
			}

		case "status_500":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"error": map[string]any{
					"message": "evalreel chaos injected: internal server error",
					"type":    "server_error",
					"rule":    matched.Name,
				},
			})
			if onIntercepted != nil {
				onIntercepted(r, *matched, http.StatusInternalServerError)
			}

		default:
			next.ServeHTTP(w, r)
		}
	})
}

// MatchStreamRule 检查当前请求路径是否命中流式阶段的故障规则。
func (e *Engine) MatchStreamRule(path string) *config.ChaosRule {
	if e == nil || !e.enabled {
		return nil
	}
	return e.matchRules(e.streamRules, path)
}

func (e *Engine) matchRules(rules []compiledRule, path string) *config.ChaosRule {
	for i := range rules {
		cr := &rules[i]
		if cr.rule.MatchPath != "" && path != cr.rule.MatchPath && !strings.HasSuffix(path, cr.rule.MatchPath) {
			continue
		}
		if cr.rule.EveryN > 0 {
			if cr.counter.Add(1)%cr.rule.EveryN == 0 {
				ruleCopy := cr.rule
				return &ruleCopy
			}
			continue
		}
		if cr.rule.Probability > 0 {
			if e.randFloat() < cr.rule.Probability {
				ruleCopy := cr.rule
				return &ruleCopy
			}
		}
	}
	return nil
}

func sleepContext(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}
