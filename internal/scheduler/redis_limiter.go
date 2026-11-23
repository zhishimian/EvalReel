package scheduler

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const redisRateLimitPrefix = "evalreel:ratelimit:"

// luaTokenBucket 使用 Redis 服务端时间（TIME）原子完成补充令牌与扣减判断，
// 返回值：{是否放行(1/0), 若未放行建议等待毫秒数}。
var luaTokenBucket = redis.NewScript(`
local key = KEYS[1]
local rpm = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local ttl_ms = tonumber(ARGV[3])

local t = redis.call("TIME")
local now_ms = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

local rate_per_ms = rpm / 60000.0
local data = redis.call("HMGET", key, "tokens", "last_ms")
local tokens = tonumber(data[1])
local last_ms = tonumber(data[2])

if tokens == nil or last_ms == nil then
	tokens = burst
	last_ms = now_ms
else
	local elapsed = now_ms - last_ms
	if elapsed > 0 then
		tokens = math.min(burst, tokens + elapsed * rate_per_ms)
		last_ms = now_ms
	end
end

if tokens >= 1.0 then
	tokens = tokens - 1.0
	redis.call("HMSET", key, "tokens", tostring(tokens), "last_ms", tostring(last_ms))
	redis.call("PEXPIRE", key, ttl_ms)
	return {1, 0}
end

local wait_ms = math.ceil((1.0 - tokens) / rate_per_ms)
if wait_ms < 1 then
	wait_ms = 1
end
redis.call("HMSET", key, "tokens", tostring(tokens), "last_ms", tostring(last_ms))
redis.call("PEXPIRE", key, ttl_ms)
return {0, wait_ms}
`)

// RedisLuaLimiter 基于 Redis Lua 脚本实现跨多台 Worker 共享的 RPM 令牌桶限流器。
type RedisLuaLimiter struct {
	client       *redis.Client
	mu           sync.RWMutex
	rpmByTarget  map[string]int
	defaultBurst int
}

// NewRedisLuaLimiter 创建跨节点分布式限流器，默认突发桶大小为 1。
func NewRedisLuaLimiter(client *redis.Client, rpmByTarget map[string]int) *RedisLuaLimiter {
	return NewRedisLuaLimiterWithBurst(client, rpmByTarget, 1)
}

// NewRedisLuaLimiterWithBurst 创建指定突发桶大小的跨节点分布式限流器。
func NewRedisLuaLimiterWithBurst(client *redis.Client, rpmByTarget map[string]int, burst int) *RedisLuaLimiter {
	if burst <= 0 {
		burst = 1
	}
	copied := make(map[string]int, len(rpmByTarget))
	for k, v := range rpmByTarget {
		copied[k] = v
	}
	return &RedisLuaLimiter{
		client:       client,
		rpmByTarget:  copied,
		defaultBurst: burst,
	}
}

// Wait 实现 Limiter 接口，按初始化时配置的 targetName RPM 上限阻塞等待放行。
func (l *RedisLuaLimiter) Wait(ctx context.Context, targetName string) error {
	if l == nil || l.client == nil {
		return nil
	}
	l.mu.RLock()
	rpm := l.rpmByTarget[targetName]
	l.mu.RUnlock()

	if rpm <= 0 {
		return nil
	}
	return l.WaitFor(ctx, targetName, rpm, l.defaultBurst)
}

// WaitFor 允许 Worker 在运行期根据不同任务的模型配置动态指定限流键、RPM 与突发容量。
func (l *RedisLuaLimiter) WaitFor(ctx context.Context, targetKey string, rpm int, burst int) error {
	if l == nil || l.client == nil || rpm <= 0 {
		return nil
	}
	if burst <= 0 {
		burst = l.defaultBurst
		if burst <= 0 {
			burst = 1
		}
	}

	ttlMs := int64(burst) * 60000 / int64(rpm) * 2
	if ttlMs < 60000 {
		ttlMs = 60000
	}

	redisKey := redisRateLimitPrefix + targetKey
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		res, err := luaTokenBucket.Run(ctx, l.client, []string{redisKey}, rpm, burst, ttlMs).Int64Slice()
		if err != nil {
			return fmt.Errorf("redis lua limiter %s: %w", targetKey, err)
		}
		if len(res) >= 2 && res[0] == 1 {
			return nil
		}

		waitMs := int64(5)
		if len(res) >= 2 && res[1] > waitMs {
			waitMs = res[1]
		}
		// 加少量随机抖动，避免多个 Worker 在同一毫秒齐刷刷醒来同时争抢。
		jitter := int64(rand.IntN(15))
		if !sleepWithContext(ctx, time.Duration(waitMs+jitter)*time.Millisecond) {
			return ctx.Err()
		}
	}
}
