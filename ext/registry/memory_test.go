package registry

import (
	"context"
	"sync"
	"testing"
	"time"
)

// 生命周期：注册、摘除与终态空帧。
func TestMemoryLifecycle(t *testing.T) {
	r := NewMemory()
	ctx := context.Background()

	ch := &snapCh{}
	if err := r.Watch(ctx, "game", withSnap(ch)); err != nil {
		t.Fatal(err)
	}

	dA, err := r.Register(ctx, "game", Endpoint{Addr: "10.0.0.1:7001"})
	if err != nil {
		t.Fatal(err)
	}
	dB, err := r.Register(ctx, "game", Endpoint{Addr: "10.0.0.2:7001"})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		return sameSet(ch.snapshot(), []Endpoint{{Addr: "10.0.0.1:7001"}, {Addr: "10.0.0.2:7001"}})
	})

	dA()
	waitFor(t, func() bool { return sameSet(ch.snapshot(), []Endpoint{{Addr: "10.0.0.2:7001"}}) })

	// 二次 dereg 为 no-op
	n := ch.frames()
	dA()
	if got := ch.frames(); got != n {
		t.Fatalf("double dereg grew frames: %d -> %d", n, got)
	}

	// Close 同步交付终态空快照
	r.Close()
	if snap := ch.snapshot(); len(snap) != 0 {
		t.Fatalf("final snapshot = %+v, want empty", snap)
	}
	// 关闭后注册与订阅均被拒绝
	if _, err := r.Register(ctx, "game", Endpoint{Addr: "x"}); err != ErrClosed {
		t.Fatalf("want ErrClosed, got %v", err)
	}
	if err := r.Watch(ctx, "game", nil); err != ErrClosed {
		t.Fatalf("want ErrClosed, got %v", err)
	}
	dB() // 关闭后 dereg 也无 panic
}

// 同 (name, Addr) 覆盖更新；任一句柄 dereg 即下线。
func TestMemoryDuplicateSameAddr(t *testing.T) {
	r := NewMemory()
	ctx := context.Background()

	ch := &snapCh{}
	if err := r.Watch(ctx, "game", withSnap(ch)); err != nil {
		t.Fatal(err)
	}

	d1, _ := r.Register(ctx, "game", Endpoint{Addr: "a:1", Weight: 1})
	d2, _ := r.Register(ctx, "game", Endpoint{Addr: "a:1", Weight: 7})

	waitFor(t, func() bool {
		s := ch.snapshot()
		return len(s) == 1 && s[0].Weight == 7
	})

	d1() // 任一句柄生效即下线
	waitFor(t, func() bool { return len(ch.snapshot()) == 0 })

	// 另一句柄再 dereg 无事件
	n := ch.frames()
	d2()
	if got := ch.frames(); got != n {
		t.Fatalf("handle 2 dereg after removal grew frames: %d -> %d", n, got)
	}
	r.Close()
}

// lease：ctx 结束自动下线并通知。
func TestMemoryLeaseCtxCancel(t *testing.T) {
	r := NewMemory()
	ch := &snapCh{}
	if err := r.Watch(context.Background(), "game", withSnap(ch)); err != nil {
		t.Fatal(err)
	}

	regCtx, cancel := context.WithCancel(context.Background())
	if _, err := r.Register(regCtx, "game", Endpoint{Addr: "a:1"}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return len(ch.snapshot()) == 1 })

	cancel()
	waitFor(t, func() bool { return len(ch.snapshot()) == 0 })
	r.Close()
}

// watcher ctx 结束即摘除，后续扰动不再收帧。
func TestMemoryWatcherCtxCancel(t *testing.T) {
	r := NewMemory()
	ctx, cancel := context.WithCancel(context.Background())
	ch := &snapCh{}
	if err := r.Watch(ctx, "game", withSnap(ch)); err != nil {
		t.Fatal(err)
	}

	// 先收一帧再取消，保证时序确定
	dA, err := r.Register(context.Background(), "game", Endpoint{Addr: "a:1"})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return len(ch.snapshot()) == 1 })
	cancel()
	dA()
	time.Sleep(50 * time.Millisecond) // 给派发协程机会

	// 取消后的扰动不得再递帧
	n := ch.frames()
	for i := 0; i < 3; i++ {
		d, err := r.Register(context.Background(), "game", Endpoint{Addr: "b:" + string(rune('0'+i))})
		if err != nil {
			t.Fatal(err)
		}
		d()
	}
	time.Sleep(50 * time.Millisecond)
	if got := ch.frames(); got != n {
		t.Fatalf("canceled watcher still received frames: %d -> %d", n, got)
	}
	r.Close()
}

// 并发压力：乱序注册/摘除，最终收敛为空。
func TestMemoryStress(t *testing.T) {
	r := NewMemory()
	ctx := context.Background()
	ch := &snapCh{}
	if err := r.Watch(ctx, "game", withSnap(ch)); err != nil {
		t.Fatal(err)
	}

	const workers = 8
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				d, err := r.Register(ctx, "game", Endpoint{Addr: string(rune('a'+w%6)) + ":" + string(rune('0'+i%10))})
				if err != nil {
					return
				}
				if i%2 == 0 {
					d()
				}
			}
		}(w)
	}
	wg.Wait()

	// 句柄已丢，直接 Close 收敛
	r.Close()
	if snap := ch.snapshot(); len(snap) != 0 {
		t.Fatalf("final snapshot = %+v, want empty", snap)
	}
}

// 多 name 隔离。
func TestMemoryNamesIsolated(t *testing.T) {
	r := NewMemory()
	ctx := context.Background()
	g := &snapCh{}
	l := &snapCh{}
	if err := r.Watch(ctx, "game", withSnap(g)); err != nil {
		t.Fatal(err)
	}
	if err := r.Watch(ctx, "lobby", withSnap(l)); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Register(ctx, "game", Endpoint{Addr: "g:1"}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return len(g.snapshot()) == 1 && len(l.snapshot()) == 0 })

	if _, err := r.Register(ctx, "lobby", Endpoint{Addr: "l:1"}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return len(l.snapshot()) == 1 })
	r.Close()
}

func keyOf(eps []Endpoint) map[string]int {
	out := map[string]int{}
	for _, e := range eps {
		out[e.Addr] = out[e.Addr] + 1
	}
	return out
}

func sameSet(a, b []Endpoint) bool {
	ka, kb := keyOf(a), keyOf(b)
	if len(ka) != len(kb) {
		return false
	}
	for k, v := range ka {
		if kb[k] != v {
			return false
		}
	}
	return true
}

type snapCh struct {
	mx  sync.Mutex
	cur []Endpoint
	n   int
}

func withSnap(ch *snapCh) func([]Endpoint) {
	return func(eps []Endpoint) {
		ch.mx.Lock()
		defer ch.mx.Unlock()
		ch.cur = eps
		ch.n++
	}
}

func (c *snapCh) frames() int {
	c.mx.Lock()
	defer c.mx.Unlock()
	return c.n
}

func (c *snapCh) snapshot() []Endpoint {
	c.mx.Lock()
	defer c.mx.Unlock()
	return c.cur
}

func waitFor(t *testing.T, pred func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if pred() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("wait timeout")
}
