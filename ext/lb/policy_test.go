package lb

import (
	"math"
	"testing"

	"github.com/segdumping/leanwire/ext/registry"
)

func endpoints(addrs ...string) []registry.Endpoint {
	out := make([]registry.Endpoint, 0, len(addrs))
	for _, a := range addrs {
		out = append(out, registry.Endpoint{Addr: a})
	}
	return out
}

func TestRoundRobin(t *testing.T) {
	p := NewRoundRobin()
	set := endpoints("a", "b", "c")
	for i := 0; i < 6; i++ {
		endpoint, err := p.Pick(set)
		if err != nil {
			t.Fatal(err)
		}
		if want := set[i%3].Addr; endpoint.Addr != want {
			t.Fatalf("pick #%d = %s, want %s", i, endpoint.Addr, want)
		}
	}
}

func TestRandomReturnMember(t *testing.T) {
	p := NewRandom()
	set := endpoints("a", "b")
	for i := 0; i < 100; i++ {
		endpoint, err := p.Pick(set)
		if err != nil {
			t.Fatal(err)
		}
		if endpoint.Addr != "a" && endpoint.Addr != "b" {
			t.Fatalf("unexpected endpoint %q", endpoint.Addr)
		}
	}
}

func TestWeighted(t *testing.T) {
	p := NewWeighted()
	set := []registry.Endpoint{{Addr: "a", Weight: 1}, {Addr: "b", Weight: 2}}
	got := map[string]int{}
	for i := 0; i < 300; i++ {
		endpoint, err := p.Pick(set)
		if err != nil {
			t.Fatal(err)
		}
		got[endpoint.Addr]++
	}
	if got["a"] != 100 || got["b"] != 200 {
		t.Fatalf("weighted counts = %v, want a=100 b=200", got)
	}
}

// 极端权重：int64 累加 + 按候选数钳位，不溢出为负、不退化为只取最后一个。
func TestWeightedExtremeWeights(t *testing.T) {
	p := NewWeighted()
	// 掺杂小权重：r 落在尾部候选的份额里，钳位后仍可被选中
	set := []registry.Endpoint{
		{Addr: "a", Weight: 1},
		{Addr: "b", Weight: 1},
		{Addr: "c", Weight: math.MaxInt32},
	}
	got := map[string]int{}
	for i := 0; i < 300; i++ {
		endpoint, err := p.Pick(set)
		if err != nil {
			t.Fatal(err)
		}
		got[endpoint.Addr]++
	}
	for _, addr := range []string{"a", "b", "c"} {
		if got[addr] == 0 {
			t.Fatalf("%s never picked: %v", addr, got)
		}
	}

	// 全极端权重：钳位累计不溢出，选取恒成功且不越候选集
	extreme := []registry.Endpoint{
		{Addr: "a", Weight: math.MaxInt32},
		{Addr: "b", Weight: math.MaxInt32},
		{Addr: "c", Weight: math.MaxInt32},
	}
	for i := 0; i < 100; i++ {
		if _, err := p.Pick(extreme); err != nil {
			t.Fatalf("pick #%d: %v", i, err)
		}
	}
}

func TestEmptySet(t *testing.T) {
	for _, p := range []Picker{NewRoundRobin(), NewRandom(), NewWeighted()} {
		if _, err := p.Pick(nil); err != ErrNoEndpoint {
			t.Fatalf("empty pick err = %v, want ErrNoEndpoint", err)
		}
	}
}
