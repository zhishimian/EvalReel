package scheduler

import (
	"context"
	"fmt"
	"testing"
	"time"

	"evalreel/internal/config"
	"evalreel/internal/store"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestRedisClient(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	cli := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() {
		_ = cli.Close()
	})
	return mr, cli
}

func TestRedisLuaLimiter_RateAndCancel(t *testing.T) {
	_, cli := newTestRedisClient(t)
	// 600 RPM = 每秒 10 次 = 100ms 补充 1 个令牌；burst=1 时连续 4 次至少间隔约 300ms。
	lim := NewRedisLuaLimiterWithBurst(cli, map[string]int{"mock-model": 600}, 1)

	ctx := context.Background()
	start := time.Now()
	for i := 0; i < 4; i++ {
		if err := lim.Wait(ctx, "mock-model"); err != nil {
			t.Fatalf("Wait(%d): %v", i, err)
		}
	}
	elapsed := time.Since(start)
	if elapsed < 220*time.Millisecond {
		t.Fatalf("expected distributed limit >= 220ms for 4 requests at 600 RPM, got %v", elapsed)
	}

	// 验证上下文取消时能够立即打断等待，不会死等令牌。
	slowLim := NewRedisLuaLimiterWithBurst(cli, map[string]int{"very-slow": 6}, 1)
	_ = slowLim.Wait(ctx, "very-slow")
	cCtx, cancel := context.WithTimeout(ctx, 40*time.Millisecond)
	defer cancel()
	if err := slowLim.Wait(cCtx, "very-slow"); err == nil {
		t.Fatalf("expected context deadline error when canceled")
	}
}

func TestRedisProgressBus_PubSub(t *testing.T) {
	_, cli := newTestRedisClient(t)
	bus := NewRedisProgressBus(cli)
	defer bus.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch, unsub, err := bus.SubscribeEvents(ctx, "run-bus-1")
	if err != nil {
		t.Fatalf("SubscribeEvents: %v", err)
	}
	defer unsub()

	for i := 1; i <= 3; i++ {
		st := store.RunRunning
		if i == 3 {
			st = store.RunCompleted
		}
		if err := bus.PublishEvent(ctx, ProgressEvent{
			RunID:  "run-bus-1",
			Total:  3,
			Done:   i,
			Status: st,
		}); err != nil {
			t.Fatalf("PublishEvent(%d): %v", i, err)
		}
	}

	for i := 1; i <= 3; i++ {
		select {
		case ev := <-ch:
			if ev.Done != i {
				t.Fatalf("expected Done=%d, got %d", i, ev.Done)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for event %d", i)
		}
	}
}

func TestAsynqDispatcher_EnqueueAndStats(t *testing.T) {
	mr, _ := newTestRedisClient(t)
	disp := NewAsynqDispatcher(mr.Addr(), "", 0)
	defer disp.Close()

	ctx := context.Background()
	tasks := make([]store.EvalTask, 5)
	for i := 0; i < 5; i++ {
		tasks[i] = store.EvalTask{
			ID:     fmt.Sprintf("run-asynq-tk-%d", i),
			RunID:  "run-asynq",
			ItemID: fmt.Sprintf("i-%d", i),
		}
	}
	cfg := config.EvalConfig{MaxRetries: 2, TaskTimeoutSec: 30}
	if err := disp.DispatchRun(ctx, "run-asynq", tasks, cfg); err != nil {
		t.Fatalf("DispatchRun: %v", err)
	}
	// 相同 TaskID 重复投递必须幂等跳过不报错。
	if err := disp.DispatchRun(ctx, "run-asynq", tasks, cfg); err != nil {
		t.Fatalf("duplicate DispatchRun should be idempotent: %v", err)
	}

	stats, err := disp.GetQueueStats(ctx)
	if err != nil {
		t.Fatalf("GetQueueStats: %v", err)
	}
	if stats.Pending != 5 {
		t.Fatalf("expected Pending=5, got %+v", stats)
	}

	// 验证 RedispatchTasks 可以覆盖去重并安全调用 ListArchivedTasks / RetryArchivedTasks。
	if err := disp.RedispatchTasks(ctx, "run-asynq", tasks[:2], cfg); err != nil {
		t.Fatalf("RedispatchTasks: %v", err)
	}
	archived, err := disp.ListArchivedTasks(ctx, 10)
	if err != nil || len(archived) != 0 {
		t.Fatalf("expected 0 archived tasks, got %+v err=%v", archived, err)
	}
	retried, err := disp.RetryArchivedTasks(ctx, nil, true)
	if err != nil || retried != 0 {
		t.Fatalf("expected 0 retried archived tasks, got %d err=%v", retried, err)
	}
}
