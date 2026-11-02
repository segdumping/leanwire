package registry

import (
	"context"
	"errors"
	"testing"
	"time"
)

func waitEndpoints(t *testing.T, r *Resolver, n int) []Endpoint {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		endpoints := r.Endpoints()
		if len(endpoints) == n {
			return endpoints
		}
		if time.Now().After(deadline) {
			t.Fatalf("resolver endpoints = %d, want %d", len(endpoints), n)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func TestResolverSnapshot(t *testing.T) {
	m := NewMemory()
	defer m.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	r := NewResolver(ctx, m, "service")
	dereg, err := m.Register(ctx, "service", Endpoint{Addr: "10.0.0.1:9000"})
	if err != nil {
		t.Fatal(err)
	}
	endpoints := waitEndpoints(t, r, 1)
	if endpoints[0].Addr != "10.0.0.1:9000" {
		t.Fatalf("endpoint = %+v", endpoints[0])
	}
	endpoint, err := r.Endpoint()
	if err != nil || endpoint.Addr != "10.0.0.1:9000" {
		t.Fatalf("Endpoint = %+v, %v", endpoint, err)
	}

	dereg()
	waitEndpoints(t, r, 0)
	if _, err := r.Endpoint(); err != ErrNoEndpoint {
		t.Fatalf("empty Endpoint err = %v, want ErrNoEndpoint", err)
	}
}

func TestResolverNilRegistry(t *testing.T) {
	r := NewResolver(context.Background(), nil, "service")
	if _, err := r.Endpoint(); err != ErrNoEndpoint {
		t.Fatalf("Endpoint err = %v, want ErrNoEndpoint", err)
	}
	if !r.Ready() {
		t.Fatal("nil registry must be settled immediately")
	}
	if _, err := r.Wait(context.Background()); err != ErrNoEndpoint {
		t.Fatalf("Wait err = %v, want ErrNoEndpoint", err)
	}
}

// 未同步与"无实例"可区分：首帧前为未同步态（ErrNotSynced），
// 首帧空快照起为"已同步但无实例"（空集 + nil）。
func TestResolverUnsyncedBarrier(t *testing.T) {
	stub := &stubNotifier{}
	r := NewResolver(context.Background(), stub, "game")
	if stub.onAsync == nil {
		t.Fatal("resolver must prefer err-reporting registries")
	}
	if r.Ready() {
		t.Fatal("must be unsynced before first snapshot")
	}
	if r.Err() != ErrNotSynced {
		t.Fatalf("Err = %v, want ErrNotSynced", r.Err())
	}
	wctx, wcancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer wcancel()
	if _, err := r.Wait(wctx); err != context.DeadlineExceeded {
		t.Fatalf("Wait err = %v, want DeadlineExceeded", err)
	}

	// 首帧（空快照）：屏障打开，空集且 Err 为 nil
	stub.onSnap([]Endpoint{})
	if !r.Ready() {
		t.Fatal("first snapshot must open ready barrier")
	}
	endpoints, err := r.Wait(context.Background())
	if err != nil || endpoints == nil || len(endpoints) != 0 {
		t.Fatalf("Wait = %+v, %v, want empty synced state", endpoints, err)
	}
	if eps := r.Endpoints(); len(eps) != 0 {
		t.Fatalf("endpoints = %+v", eps)
	}
}

// stubNotifier 模拟 watch 异步失败（etcd 的 ErrNotifier 行为）。
type stubNotifier struct {
	onSnap  func([]Endpoint)
	onAsync func(err error)
	syncErr error
}

func (s *stubNotifier) Register(context.Context, string, Endpoint) (func(), error) {
	return nil, ErrReadOnly
}

func (s *stubNotifier) Watch(context.Context, string, func([]Endpoint)) error { return nil }

func (s *stubNotifier) WatchErr(_ context.Context, _ string, callback func([]Endpoint), onError func(err error)) error {
	s.onSnap = callback
	s.onAsync = onError
	return s.syncErr
}

// watch 异步失败上抛后：Err 置错（不再把过期列表当健康态），成功快照到达即复位。
func TestResolverAsyncError(t *testing.T) {
	stub := &stubNotifier{}
	r := NewResolver(context.Background(), stub, "game")
	if stub.onSnap == nil || stub.onAsync == nil {
		t.Fatal("WatchErr not wired")
	}

	appErr := errors.New("watch lost")
	stub.onAsync(appErr)
	if !r.Ready() {
		t.Fatal("async error must open ready barrier")
	}
	if r.Err() == nil {
		t.Fatal("async error must surface via Err")
	}
	if _, err := r.Wait(context.Background()); err == nil {
		t.Fatal("Wait must return the surfaced error")
	}

	// 异步失败期间的快照仍可用于兜底重拨
	stub.onSnap([]Endpoint{{Addr: "a:1"}})
	if eps := r.Endpoints(); len(eps) != 1 {
		t.Fatalf("endpoints = %+v", eps)
	}

	// 成功快照到达恢复健康态
	stub.onSnap([]Endpoint{{Addr: "a:2"}})
	if r.Err() != nil {
		t.Fatalf("Err after successful snapshot = %v", r.Err())
	}
}
