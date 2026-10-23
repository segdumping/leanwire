package registry

import (
	"context"
	"errors"
	"sort"
	"sync"
)

var ErrReadOnly = errors.New("registry: static registry is read-only")

type Static struct {
	mu sync.Mutex
	m  map[string][]Endpoint
}

func NewStatic(byName map[string][]Endpoint) *Static {
	s := &Static{m: make(map[string][]Endpoint, len(byName))}
	for name, endpoints := range byName {
		kept := make([]Endpoint, 0, len(endpoints))
		for _, endpoint := range endpoints {
			if endpoint.Addr == "" {
				panic("registry: NewStatic endpoint with empty addr")
			}
			kept = append(kept, endpoint.clone())
		}
		s.m[name] = kept
	}
	for name := range s.m {
		endpoints := s.m[name]
		sort.Slice(endpoints, func(i, j int) bool { return endpoints[i].Addr < endpoints[j].Addr })
	}
	return s
}

func (s *Static) Register(context.Context, string, Endpoint) (func(), error) {
	return nil, ErrReadOnly
}

func (s *Static) Watch(_ context.Context, name string, callback func([]Endpoint)) error {
	s.mu.Lock()
	endpoints := s.m[name]
	snap := make([]Endpoint, len(endpoints))
	copy(snap, endpoints)
	s.mu.Unlock()
	if callback != nil {
		callback(snap)
	}
	return nil
}
