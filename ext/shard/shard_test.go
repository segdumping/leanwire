package shard

import (
	"context"
	"sync"
	"testing"
	"time"
)

// 同一 key 严格串行（跨并发提交）。
func TestKeyOrdering(t *testing.T) {
	p := New(4)
	defer func() { _ = p.Drain(context.Background()) }()

	const perKey = 100
	keys := 8

	var mu sync.Mutex
	seen := make(map[uint64][]int, keys)
	var wg sync.WaitGroup
	wg.Add(keys)
	for k := 0; k < keys; k++ {
		key := uint64(k)
		// 每个键独立计数，全局乱序由提交方保证
		for i := 0; i < perKey; i++ {
			idx := i
			if ok := p.TryPost(key, func() {
				mu.Lock()
				seen[key] = append(seen[key], idx)
				mu.Unlock()
				if idx == perKey-1 {
					wg.Done()
				}
			}); !ok {
				t.Fatalf("submit lost at key=%d idx=%d", key, idx)
			}
		}
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	for k := 0; k < keys; k++ {
		ord := seen[uint64(k)]
		if len(ord) != perKey {
			t.Fatalf("key %d: %d tasks, want %d", k, len(ord), perKey)
		}
		for i, v := range ord {
			if v != i {
				t.Fatalf("key %d out of order at %d: got %d", k, i, v)
			}
		}
	}
}

// panic 只废一个任务，不伤 worker，后续任务照常。
func TestPanicIsolated(t *testing.T) {
	panicSeen := make(chan any, 1)
	p := New(2, WithPanicHook(func(key uint64, r any) { panicSeen <- r }))
	defer func() { _ = p.Drain(context.Background()) }()

	done := make(chan struct{})
	if !p.TryPost(1, func() { panic("boom") }) {
		t.Fatal("submit panic task failed")
	}
	if !p.TryPost(1, func() { close(done) }) {
		t.Fatal("submit follow-up failed")
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("task after panic never ran")
	}
	select {
	case r := <-panicSeen:
		if r != "boom" {
			t.Fatalf("panic hook got %v", r)
		}
	default:
		t.Fatal("panic hook not called")
	}
}

// Drain 后 Run 返回 false；在途任务全部完成。
func TestDrainStopsAcceptance(t *testing.T) {
	p := New(2)
	completed := make(chan struct{}, 4)
	if !p.TryPost(0, func() { completed <- struct{}{} }) {
		t.Fatal("submit failed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := p.Drain(ctx); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if ok := p.TryPost(0, func() {}); ok {
		t.Fatal("Run after drain must return false")
	}
	select {
	case <-completed:
	default:
		t.Fatal("in-flight task lost during drain")
	}
}

// 二次 Drain 幂等。
func TestDrainIdempotent(t *testing.T) {
	p := New(2)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := p.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if err := p.Drain(ctx); err != nil {
		t.Fatal(err)
	}
}

// 队列满：Run 返回 false，不阻塞。
func TestQueueFullNonBlocking(t *testing.T) {
	p := New(2, WithQueueCap(1))
	defer func() { _ = p.Drain(context.Background()) }()

	started := make(chan struct{})
	release := make(chan struct{})
	if ok := p.TryPost(0, func() {
		close(started)
		<-release
	}); !ok {
		t.Fatal("submit slow task failed")
	}
	<-started // 慢任务已被取走，队列空出

	if ok := p.TryPost(0, func() {}); !ok {
		t.Fatal("submit filler failed (queue cap=1 should hold one)")
	}
	if ok := p.TryPost(0, func() {}); ok {
		t.Fatal("queue-full Run must return false")
	}
	close(release)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := p.Drain(ctx); err != nil {
		t.Fatalf("drain: %v", err)
	}
}
