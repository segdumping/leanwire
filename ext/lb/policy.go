package lb

import (
	"context"
	"errors"
	"math/rand/v2"
	"net"
	"sync/atomic"
	"time"

	"github.com/segdumping/leanwire/ext/registry"
)

// ErrNoEndpoint 候选集为空。
var ErrNoEndpoint = errors.New("lb: no endpoint")

type DialFunc func(ctx context.Context, addr string, timeout time.Duration) (net.Conn, error)

type Picker interface {
	Pick(endpoints []registry.Endpoint) (registry.Endpoint, error)
}

// NewRoundRobin 轮转选择。
func NewRoundRobin() Picker { return &roundRobin{} }

type roundRobin struct{ n atomic.Uint64 }

func (r *roundRobin) Pick(endpoints []registry.Endpoint) (registry.Endpoint, error) {
	if len(endpoints) == 0 {
		return registry.Endpoint{}, ErrNoEndpoint
	}
	i := (r.n.Add(1) - 1) % uint64(len(endpoints))
	return endpoints[i], nil
}

// NewRandom 随机选择。
func NewRandom() Picker { return random{} }

type random struct{}

func (random) Pick(endpoints []registry.Endpoint) (registry.Endpoint, error) {
	if len(endpoints) == 0 {
		return registry.Endpoint{}, ErrNoEndpoint
	}
	return endpoints[rand.IntN(len(endpoints))], nil
}

// NewWeighted 带权轮转。
func NewWeighted() Picker { return &weighted{} }

type weighted struct{ n atomic.Uint64 }

func (w *weighted) Pick(endpoints []registry.Endpoint) (registry.Endpoint, error) {
	if len(endpoints) == 0 {
		return registry.Endpoint{}, ErrNoEndpoint
	}
	total := 0
	for _, endpoint := range endpoints {
		if endpoint.Weight > 0 {
			total += endpoint.Weight
		} else {
			total++
		}
	}
	r := int((w.n.Add(1)-1)%uint64(total)) + 1
	for idx := range endpoints {
		wt := endpoints[idx].Weight
		if wt <= 0 {
			wt = 1
		}
		r -= wt
		if r <= 0 {
			return endpoints[idx], nil
		}
	}
	return endpoints[len(endpoints)-1], nil
}
