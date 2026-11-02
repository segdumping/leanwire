package codec

import (
	"bytes"
	"go/parser"
	"go/token"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// golden：大端字节与帧格式逐字节一致。
func TestGolden(t *testing.T) {
	c := DefaultCodec(1024)
	m := Msg{ID: 0x1234, Body: []byte("hi")}

	var buffer bytes.Buffer
	if err := c.WriteMsg(&buffer, m); err != nil {
		t.Fatal(err)
	}
	want := []byte{0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x12, 0x34, 'h', 'i'}
	if !bytes.Equal(buffer.Bytes(), want) {
		t.Fatalf("golden mismatch:\n got % x\nwant % x", buffer.Bytes(), want)
	}

	got, err := c.ReadMsg(bytes.NewReader(want))
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != m.ID || !bytes.Equal(got.Body, m.Body) {
		t.Fatalf("roundtrip mismatch: %+v", got)
	}
}

// 超限：读写两侧均返回 ErrTooLarge。
func TestTooLarge(t *testing.T) {
	const max = 8
	c := DefaultCodec(max)

	var buffer bytes.Buffer
	if err := c.WriteMsg(&buffer, Msg{ID: 1, Body: make([]byte, max+1)}); err != ErrTooLarge {
		t.Fatalf("write: want ErrTooLarge, got %v", err)
	}

	// 线上伪造超长 len
	frame := []byte{0, 0, 0, max + 1, 0, 0, 0, 1}
	if _, err := c.ReadMsg(bytes.NewReader(frame)); err != ErrTooLarge {
		t.Fatalf("read: want ErrTooLarge, got %v", err)
	}
}

// 空消息往返。
func TestEmptyBody(t *testing.T) {
	c := DefaultCodec(1024)
	var buffer bytes.Buffer
	if err := c.WriteMsg(&buffer, Msg{ID: 0xABCD}); err != nil {
		t.Fatal(err)
	}
	want := []byte{0, 0, 0, 0, 0x00, 0x00, 0xAB, 0xCD}
	if !bytes.Equal(buffer.Bytes(), want) {
		t.Fatalf("got % x, want % x", buffer.Bytes(), want)
	}
	m, err := c.ReadMsg(bytes.NewReader(want))
	if err != nil || m.ID != 0xABCD || len(m.Body) != 0 {
		t.Fatalf("got %+v, %v", m, err)
	}
}

// 保留段常量值与边界判断。
func TestReservedID(t *testing.T) {
	if ReservedIDMin != 0xF000 || ReservedIDMax != 0xFFFF {
		t.Fatalf("segment bounds: %#x–%#x", ReservedIDMin, ReservedIDMax)
	}
	cases := map[uint32]bool{
		0xEFFF:  false, // 保留段前一位
		0xF000:  true,  // 段首
		0xF001:  true,  // heartbeat
		0xF002:  true,  // rpc
		0xF005:  true,  // TUN
		0xF006:  true,  // app 请求复用通道
		0xFFFF:  true,  // 段尾
		0x10000: false,
		0x1:     false,
	}
	for id, want := range cases {
		if got := IsReserved(id); got != want {
			t.Fatalf("IsReserved(%#08x) = %v, want %v", id, got, want)
		}
	}
}

// 保留号登记：插件取号的唯一事实源，须落在保留段内且互不撞号。
func TestReservedIDRegistry(t *testing.T) {
	ids := map[string]uint32{
		"IDHeartbeat": IDHeartbeat,
		"IDRPC":       IDRPC,
		"IDTUN":       IDTUN,
	}
	seen := make(map[uint32]string, len(ids))
	for name, id := range ids {
		if !IsReserved(id) {
			t.Fatalf("%s = %#x 不在保留段内", name, id)
		}
		if other, dup := seen[id]; dup {
			t.Fatalf("%s 与 %s 撞号 %#x", name, other, id)
		}
		seen[id] = name
	}
}

// TestKernelPurity 机检内核纯净性：codec 是零依赖内核，不得 import 本仓库
// 任何包（含 ext/ 插件），保证"插件可删除"的结构承诺有自动化验证。
func TestKernelPurity(t *testing.T) {
	entries, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	for _, path := range entries {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		for _, im := range f.Imports {
			p := strings.Trim(im.Path.Value, `"`)
			if strings.Contains(p, "segdumping/leanwire") {
				t.Errorf("%s imports %s: kernel codec must stay dependency-free", path, p)
			}
		}
	}
}

// TestKernelNoExtDeps 机检内核依赖闭包：codec 与 server 不得传递依赖 ext 包
// （删得掉纪律：内核对插件零依赖，插件由 app 组合）。
func TestKernelNoExtDeps(t *testing.T) {
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("go toolchain unavailable")
	}
	out, err := exec.Command("go", "list", "-deps",
		"github.com/segdumping/leanwire/codec", "github.com/segdumping/leanwire/server").CombinedOutput()
	if err != nil {
		t.Skipf("go list unavailable: %v (%s)", err, out)
	}
	const extPrefix = "github.com/segdumping/leanwire/ext"
	for _, line := range strings.Split(string(out), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, extPrefix) {
			t.Fatalf("内核依赖了 ext 包（破坏删得掉纪律）: %s", line)
		}
	}
}
