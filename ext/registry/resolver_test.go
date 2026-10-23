package registry

import (
	"context"
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
}
