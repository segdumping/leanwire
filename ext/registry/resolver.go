package registry

import (
	"context"
	"errors"
	"sync"
)

// ErrNoEndpoint 当前没有可用 endpoint。
var ErrNoEndpoint = errors.New("registry: no endpoint")

// ErrNotSynced 首帧快照尚未到达：与 ErrNoEndpoint 区分"未同步"与"无实例"。
var ErrNotSynced = errors.New("registry: resolver not synced")

// ErrNotifier 目录实现支持上抛 watch 异步错误的可选能力。
// Watch 的同步错误走返回值；此后首帧拉取失败与断流重连间隙经 onError 上抛（可重复），
// 观察方据此判定"手里的列表是否过期"，下次成功快照自然恢复健康态。
type ErrNotifier interface {
	// WatchErr 行为同 Watch，另在异步失败时上抛错误。
	WatchErr(ctx context.Context, name string, callback func([]Endpoint), onError func(err error)) error
}

type Resolver struct {
	mu        sync.RWMutex
	endpoints []Endpoint
	err       error

	readyOnce sync.Once
	readyCh   chan struct{} // 首帧快照或错误后关闭
}

func NewResolver(ctx context.Context, reg Registry, name string) *Resolver {
	if ctx == nil {
		ctx = context.Background()
	}
	r := &Resolver{readyCh: make(chan struct{})}
	if reg == nil {
		r.settle(ErrNoEndpoint)
		return r
	}
	if en, ok := reg.(ErrNotifier); ok {
		if err := en.WatchErr(ctx, name, r.onSnapshot, r.onAsyncErr); err != nil {
			r.settle(err)
		}
		return r
	}
	if err := reg.Watch(ctx, name, r.onSnapshot); err != nil {
		r.settle(err)
	}
	return r
}

// settle 同步失败即置错并放行等待方（不会与快照并发）。
func (r *Resolver) settle(err error) {
	r.mu.Lock()
	r.err = err
	r.mu.Unlock()
	r.readyOnce.Do(func() { close(r.readyCh) })
}

func (r *Resolver) onSnapshot(endpoints []Endpoint) {
	out := make([]Endpoint, len(endpoints))
	for i, ep := range endpoints {
		out[i] = ep.clone() // Meta 深拷贝，观察方共享的表不被外部改动
	}
	r.mu.Lock()
	r.endpoints = out
	r.err = nil
	r.mu.Unlock()
	r.readyOnce.Do(func() { close(r.readyCh) })
}

// onAsyncErr watch 异步失败：置错并放行等待方；成功快照随后到达即恢复。
func (r *Resolver) onAsyncErr(err error) {
	if err == nil {
		return
	}
	r.mu.Lock()
	r.err = err
	r.mu.Unlock()
	r.readyOnce.Do(func() { close(r.readyCh) })
}

func (r *Resolver) Endpoints() []Endpoint {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Endpoint, len(r.endpoints))
	copy(out, r.endpoints)
	return out
}

func (r *Resolver) Endpoint() (Endpoint, error) {
	endpoints := r.Endpoints()
	if len(endpoints) == 0 {
		return Endpoint{}, ErrNoEndpoint
	}
	return endpoints[0], nil
}

// Err 当前健康度：nil 即已同步且无异步失败；
// 首帧尚未到达时返回 ErrNotSynced，可区别于"已同步但无实例"。
func (r *Resolver) Err() error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.err != nil {
		return r.err
	}
	select {
	case <-r.readyCh:
		return nil
	default:
		return ErrNotSynced
	}
}

// Wait 初始同步屏障：阻塞至首帧快照或错误后，
// 返回当前 endpoint 集（可能为空列表）与当前错误——空列表 + nil 即"已同步但无实例"。
func (r *Resolver) Wait(ctx context.Context) ([]Endpoint, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-r.readyCh:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Endpoint, len(r.endpoints))
	copy(out, r.endpoints)
	return out, r.err
}

// Ready 非阻塞查询是否已过首帧同步。
func (r *Resolver) Ready() bool {
	select {
	case <-r.readyCh:
		return true
	default:
		return false
	}
}
