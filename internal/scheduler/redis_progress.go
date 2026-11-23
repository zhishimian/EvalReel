package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const redisProgressChannelPrefix = "evalreel:progress:"

// RedisProgressBus 基于 Redis Pub/Sub 实现跨容器进度广播（Worker 发布，API Server 订阅转发给 SSE）。
type RedisProgressBus struct {
	client *redis.Client
}

// NewRedisProgressBus 创建基于 Redis Pub/Sub 的分布式进度总线。
func NewRedisProgressBus(client *redis.Client) *RedisProgressBus {
	return &RedisProgressBus{client: client}
}

// PublishEvent 将进度事件序列化后发布到对应 run_id 的 Redis 频道。
func (b *RedisProgressBus) PublishEvent(ctx context.Context, ev ProgressEvent) error {
	if b == nil || b.client == nil {
		return nil
	}
	if ev.Timestamp.IsZero() {
		ev.Timestamp = time.Now().UTC()
	}
	data, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("marshal progress event: %w", err)
	}
	return b.client.Publish(ctx, redisProgressChannelPrefix+ev.RunID, string(data)).Err()
}

// SubscribeEvents 订阅 Redis 频道并转发为带缓冲的 Go 通道；取消时立即关闭订阅并等待转发协程退出。
func (b *RedisProgressBus) SubscribeEvents(ctx context.Context, runID string) (<-chan ProgressEvent, func(), error) {
	channel := redisProgressChannelPrefix + runID
	ps := b.client.Subscribe(ctx, channel)
	// 先确认 Redis 订阅握手成功再返回，防止前几条关键进度消息丢失。
	if _, err := ps.Receive(ctx); err != nil {
		_ = ps.Close()
		return nil, func() {}, fmt.Errorf("subscribe redis channel %s: %w", channel, err)
	}

	out := make(chan ProgressEvent, 64)
	done := make(chan struct{})
	var closePSOnce sync.Once
	closePS := func() {
		closePSOnce.Do(func() {
			_ = ps.Close()
		})
	}

	msgCh := ps.Channel()
	go func() {
		defer close(done)
		defer close(out)
		for {
			select {
			case msg, ok := <-msgCh:
				if !ok {
					return
				}
				var ev ProgressEvent
				if err := json.Unmarshal([]byte(msg.Payload), &ev); err != nil {
					continue
				}
				select {
				case out <- ev:
				case <-ctx.Done():
					closePS()
					return
				}
			case <-ctx.Done():
				closePS()
				return
			}
		}
	}()

	var unsubOnce sync.Once
	cancel := func() {
		unsubOnce.Do(func() {
			closePS()
			<-done
		})
	}
	return out, cancel, nil
}

// Close 保持接口一致；Redis 客户端生命周期由外部统一管理。
func (b *RedisProgressBus) Close() error {
	return nil
}
