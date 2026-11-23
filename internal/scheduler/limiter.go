package scheduler

import (
	"context"
	"sync"

	"golang.org/x/time/rate"
)

// Limiter 定义按被测目标名称进行并发发包限流的接口。
type Limiter interface {
	Wait(ctx context.Context, targetName string) error
}

// LocalLimiter 为每个被测模型独立维护一个单机令牌桶，防止把对应接口压出 429 报错。
type LocalLimiter struct {
	mu       sync.RWMutex
	limiters map[string]*rate.Limiter
}

// NewLocalLimiter 根据各目标的每分钟请求上限（RPM）创建单机限流器。
func NewLocalLimiter(rpmByTarget map[string]int) *LocalLimiter {
	return NewLocalLimiterWithBurst(rpmByTarget, 1)
}

// NewLocalLimiterWithBurst 创建指定突发桶容量的单机限流器。
func NewLocalLimiterWithBurst(rpmByTarget map[string]int, burst int) *LocalLimiter {
	if burst <= 0 {
		burst = 1
	}
	m := make(map[string]*rate.Limiter, len(rpmByTarget))
	for name, rpm := range rpmByTarget {
		if rpm > 0 {
			perSec := rate.Limit(float64(rpm) / 60.0)
			m[name] = rate.NewLimiter(perSec, burst)
		}
	}
	return &LocalLimiter{limiters: m}
}

// Wait 阻塞等待直到目标对应的令牌桶放行，或外部上下文取消。
func (l *LocalLimiter) Wait(ctx context.Context, targetName string) error {
	if l == nil {
		return nil
	}
	l.mu.RLock()
	lim := l.limiters[targetName]
	l.mu.RUnlock()

	if lim == nil {
		return nil
	}
	return lim.Wait(ctx)
}
