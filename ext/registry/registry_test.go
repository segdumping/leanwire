package registry

import "testing"

// Endpoint.clone 深拷贝 Meta；nil Meta 保持 nil。
func TestEndpointClone(t *testing.T) {
	src := Endpoint{
		Addr:   "10.0.0.1:7001",
		Weight: 2,
		Meta:   map[string]string{"zone": "a"},
	}
	got := src.clone()
	if got.Addr != src.Addr || got.Weight != src.Weight || got.Meta["zone"] != "a" {
		t.Fatalf("clone = %+v, want %+v", got, src)
	}

	// 改副本不得影响原值
	got.Meta["zone"] = "b"
	got.Meta["new"] = "x"
	if src.Meta["zone"] != "a" || len(src.Meta) != 1 {
		t.Fatalf("clone shares Meta with source: %+v", src.Meta)
	}

	// nil Meta 不分配
	if got := (Endpoint{Addr: "x"}).clone(); got.Meta != nil {
		t.Fatalf("nil Meta cloned to %v", got.Meta)
	}
}
