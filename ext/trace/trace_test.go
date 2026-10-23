package trace

// 测试验证：ID 生成/前缀编解码、Handle 装饰器拆装与透传。

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/segdumping/leanwire/codec"
	"github.com/segdumping/leanwire/ext/router"
	"github.com/segdumping/leanwire/server"
)

// capHandler 收集格式化日志行，供断言。
type capHandler struct {
	mu     sync.Mutex
	buffer strings.Builder
}

func (h *capHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *capHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	fmt.Fprintf(&h.buffer, "%s %s", r.Level, r.Message)
	r.Attrs(func(a slog.Attr) bool {
		fmt.Fprintf(&h.buffer, " %s=%v", a.Key, a.Value)
		return true
	})
	h.buffer.WriteByte('\n')
	return nil
}

func (h *capHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *capHandler) WithGroup(string) slog.Handler      { return h }

func (h *capHandler) String() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.buffer.String()
}

// waitUntil 轮询至条件成立或超时报错。
func waitUntil(cond func() bool, msg func() string) error {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("timeout: %s", msg())
}

func TestIDUniqueAndString(t *testing.T) {
	seen := make(map[ID]bool, 64)
	for i := 0; i < 64; i++ {
		id := NewID()
		if seen[id] {
			t.Fatal("duplicate ID")
		}
		seen[id] = true
		if s := id.String(); len(s) != 16 {
			t.Fatalf("String len %d, want 16", len(s))
		}
	}
}

func TestPackUnpack(t *testing.T) {
	id := NewID()
	body := []byte("hello")
	p := Pack(id, body)
	if len(p) != idLen+len(body) {
		t.Fatalf("packed len %d", len(p))
	}
	if !bytes.Equal(p[idLen:], body) || !bytes.Equal(body, []byte("hello")) {
		t.Fatal("Pack must not mutate inputs")
	}

	got, rest, err := Unpack(p)
	if err != nil || got != id || !bytes.Equal(rest, body) {
		t.Fatalf("roundtrip: %v % x", err, rest)
	}

	// 空体边界
	if _, rest, err := Unpack(Pack(id, nil)); err != nil || len(rest) != 0 {
		t.Fatalf("empty body: %v % x", err, rest)
	}
	// 恰好 8 字节
	if _, rest, err := Unpack(p[:idLen]); err != nil || len(rest) != 0 {
		t.Fatalf("exact prefix: %v", err)
	}
	// 不足前缀
	if _, _, err := Unpack([]byte{1, 2, 3}); err != ErrShortTrace {
		t.Fatalf("short: want ErrShortTrace, got %v", err)
	}
}

// Handle 装饰器：拆前缀给裸 body；无前缀透传

func TestHandleDecorator(t *testing.T) {
	capH := &capHandler{}
	testServer := server.New(server.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))))
	handle := Handle(slog.New(capH), func(s *server.Conn, m codec.Msg) {
		_ = s.Send(codec.Msg{ID: m.ID, Body: m.Body})
	})
	routeTable := router.Bind(testServer)
	routeTable.Handle(10, handle)
	routeTable.Handle(11, handle)

	serverEnd, testEnd := net.Pipe()
	if _, err := testServer.Connect(serverEnd); err != nil {
		t.Fatal(err)
	}

	c := codec.DefaultCodec(64 * 1024)
	send := func(id uint32, body []byte) codec.Msg {
		if err := c.WriteMsg(testEnd, codec.Msg{ID: id, Body: body}); err != nil {
			t.Fatal(err)
		}
		_ = testEnd.SetReadDeadline(time.Now().Add(2 * time.Second))
		m, err := c.ReadMsg(testEnd)
		if err != nil {
			t.Fatal(err)
		}
		return m
	}

	id := NewID()
	if m := send(10, Pack(id, []byte("x"))); string(m.Body) != "x" {
		t.Fatalf("handler must see bare body, got % x", m.Body)
	}
	if err := waitUntil(
		func() bool { return strings.Contains(capH.String(), "trace="+id.String()) },
		capH.String,
	); err != nil {
		t.Fatal(err)
	}

	// 无前缀：透传原 body，trace 记 "-"
	if m := send(11, []byte{7, 8}); !bytes.Equal(m.Body, []byte{7, 8}) {
		t.Fatalf("body without prefix must pass through, got % x", m.Body)
	}
	if err := waitUntil(
		func() bool { return strings.Contains(capH.String(), "trace=-") },
		capH.String,
	); err != nil {
		t.Fatal(err)
	}
}
