package registry

import (
	"context"
	"errors"
	"sync"
)

// ErrNoEndpoint 当前没有可用 endpoint。
var ErrNoEndpoint = errors.New("registry: no endpoint")

type Resolver struct {
	mu        sync.RWMutex
	endpoints []Endpoint
	err       error
}

func NewResolver(ctx context.Context, reg Registry, name string) *Resolver {
	r := &Resolver{}
	if ctx == nil {
		ctx = context.Background()
	}
	if reg == nil {
		r.err = ErrNoEndpoint
		return r
	}
	if err := reg.Watch(ctx, name, r.onSnapshot); err != nil {
		r.mu.Lock()
		r.err = err
		r.mu.Unlock()
	}
	return r
}

func (r *Resolver) onSnapshot(endpoints []Endpoint) {
	out := make([]Endpoint, len(endpoints))
	copy(out, endpoints)
	r.mu.Lock()
	r.endpoints = out
	r.err = nil
	r.mu.Unlock()
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

func (r *Resolver) Err() error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.err
}
