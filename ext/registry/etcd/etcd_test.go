package etcd

// 门控测试：需 ETCD_ENDPOINTS（逗号分隔）指向可用 etcd，未设置即 skip。

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/segdumping/leanwire/ext/registry"
)

func testEndpoints(t *testing.T) []string {
	t.Helper()
	raw := os.Getenv("ETCD_ENDPOINTS")
	var endpoints []string
	for _, p := range strings.Split(raw, ",") {
		if p = strings.TrimSpace(p); p != "" {
			endpoints = append(endpoints, p)
		}
	}
	if len(endpoints) == 0 {
		t.Skip("ETCD_ENDPOINTS not set; etcd-gated tests skipped")
	}
	return endpoints
}

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func uniquePrefix(t *testing.T) string {
	t.Helper()
	name := strings.NewReplacer("/", "-", " ", "-").Replace(t.Name())
	return fmt.Sprintf("/leanwire-it/%s-%d", name, time.Now().UnixNano())
}

func newTestRegistry(t *testing.T) *Registry {
	t.Helper()
	r, err := New(testEndpoints(t),
		WithPrefix(uniquePrefix(t)),
		WithLeaseTTL(5*time.Second),
		WithLogger(quietLogger()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.Close)
	return r
}

// snapCh 收集 watcher 帧（保留最后一帧 + 帧数 + 是否已交付过）。
type snapCh struct {
	mu     sync.Mutex
	cur    []registry.Endpoint
	have   bool
	frames int
}

func (c *snapCh) onEvent(endpoints []registry.Endpoint) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cur = endpoints
	c.have = true
	c.frames++
}

func (c *snapCh) snapshot() ([]registry.Endpoint, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cur, c.have
}

func (c *snapCh) frameCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.frames
}

func waitFor(t *testing.T, pred func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if pred() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("wait timeout")
}

// 生命周期：watch、注册、dereg、ctx 取消、Close 后拒绝。
func TestEtcdLifecycle(t *testing.T) {
	r := newTestRegistry(t)
	ctx := context.Background()

	ch := &snapCh{}
	if err := r.Watch(ctx, "game", ch.onEvent); err != nil {
		t.Fatal(err)
	}

	dA, err := r.Register(ctx, "game", registry.Endpoint{Addr: "10.0.0.1:7001"})
	if err != nil {
		t.Fatal(err)
	}
	dB, err := r.Register(ctx, "game", registry.Endpoint{Addr: "10.0.0.2:7001", Weight: 3})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { cur, _ := ch.snapshot(); return len(cur) == 2 })
	cur, _ := ch.snapshot()
	if cur[0].Addr != "10.0.0.1:7001" || cur[1].Addr != "10.0.0.2:7001" {
		t.Fatalf("snapshot not addr-sorted: %+v", cur)
	}
	if cur[1].Weight != 3 {
		t.Fatalf("weight lost: %+v", cur[1])
	}

	// 主动下线：撤销租约 → delete 事件 → 快照收敛
	dA()
	waitFor(t, func() bool { cur, _ := ch.snapshot(); return len(cur) == 1 })
	dA() // 幂等

	// 注册 ctx 结束 → 自动下线
	regCtx, cancel := context.WithCancel(ctx)
	dC, err := r.Register(regCtx, "game", registry.Endpoint{Addr: "10.0.0.3:7001"})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { cur, _ := ch.snapshot(); return len(cur) == 2 })
	cancel()
	waitFor(t, func() bool { cur, _ := ch.snapshot(); return len(cur) == 1 })
	dC() // ctx 结束后再次 dereg：幂等

	r.Close()
	if _, err := r.Register(ctx, "game", registry.Endpoint{Addr: "x:1"}); err != registry.ErrClosed {
		t.Fatalf("register after close = %v, want ErrClosed", err)
	}
	if err := r.Watch(ctx, "game", nil); err != registry.ErrClosed {
		t.Fatalf("watch after close = %v, want ErrClosed", err)
	}
	dB() // Close 后 dereg：无 panic、幂等
	r.Close()
}

// 入参校验（不触网）。
func TestEtcdValidation(t *testing.T) {
	r := newTestRegistry(t)
	ctx := context.Background()

	if _, err := r.Register(ctx, "", registry.Endpoint{Addr: "a:1"}); err == nil {
		t.Fatal("empty name must fail")
	}
	if _, err := r.Register(ctx, "game", registry.Endpoint{}); err == nil {
		t.Fatal("empty addr must fail")
	}
	if _, err := r.Register(ctx, "ga/me", registry.Endpoint{Addr: "a:1"}); err == nil {
		t.Fatal("name with '/' must fail")
	}
	if err := r.Watch(ctx, "ga/me", nil); err == nil {
		t.Fatal("watch name with '/' must fail")
	}
}

// 并发注册 × 单 watcher：交付严格串行、快照单调。
func TestEtcdWatchSerialAndMonotonic(t *testing.T) {
	r := newTestRegistry(t)
	ctx := context.Background()

	var active atomic.Int32
	var violation atomic.Bool
	var mu sync.Mutex
	prev := -1

	ch := &snapCh{}
	callback := func(endpoints []registry.Endpoint) {
		if active.Add(1) != 1 {
			violation.Store(true) // 同一 watcher 回调重入
		}
		mu.Lock()
		if prev >= 0 && len(endpoints) < prev {
			violation.Store(true) // 后帧比前帧旧
		}
		prev = len(endpoints)
		mu.Unlock()
		time.Sleep(2 * time.Millisecond) // 放大并发窗口
		active.Add(-1)
		ch.onEvent(endpoints)
	}
	if err := r.Watch(ctx, "game", callback); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 3; i++ {
				if _, err := r.Register(ctx, "game",
					registry.Endpoint{Addr: fmt.Sprintf("10.0.0.%d:%d", w, 7000+i)}); err != nil {
					t.Errorf("register: %v", err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	waitFor(t, func() bool { cur, _ := ch.snapshot(); return len(cur) == 24 })
	if violation.Load() {
		t.Fatal("契约违反：并发交付或非单调交付")
	}
}
