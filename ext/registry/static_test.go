package registry

import (
	"testing"
)

// 静态表：构造期深拷贝、快照不可变。
func TestStaticSnapshot(t *testing.T) {
	src := map[string][]Endpoint{
		"game": {
			{Addr: "10.0.0.1:7001", Meta: map[string]string{"svrid": "2001"}},
			{Addr: "10.0.0.2:7001"},
		},
	}
	r := NewStatic(src)

	// 修改入参不得影响 registry
	src["game"][0].Meta["svrid"] = "mutated"
	src["game"] = append(src["game"], Endpoint{Addr: "evil"})

	var got []Endpoint
	if err := r.Watch(t.Context(), "game", func(endpoints []Endpoint) { got = endpoints }); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Meta["svrid"] != "2001" {
		t.Fatalf("snapshot polluted: %+v", got)
	}
	// 快照按 Addr 升序
	if got[0].Addr != "10.0.0.1:7001" || got[1].Addr != "10.0.0.2:7001" {
		t.Fatalf("snapshot not addr-sorted: %+v", got)
	}

	// 未配置 name 交付非 nil 空快照
	var empty []Endpoint
	if err := r.Watch(t.Context(), "nomissing", func(endpoints []Endpoint) { empty = endpoints }); err != nil {
		t.Fatal(err)
	}
	if empty == nil || len(empty) != 0 {
		t.Fatalf("want non-nil empty snapshot, got %v", empty)
	}

	if _, err := r.Register(t.Context(), "game", Endpoint{Addr: "x"}); err != ErrReadOnly {
		t.Fatalf("want ErrReadOnly, got %v", err)
	}

	// 空 Addr 构造期 panic
	defer func() {
		if recover() == nil {
			t.Fatal("empty addr must panic at construction")
		}
	}()
	NewStatic(map[string][]Endpoint{"bad": {{Addr: ""}}})
}
