package router

// 路由表迁移自内核 Handle 家族：注册/查重/冻结/未知帧策略。

import (
	"bytes"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/segdumping/leanwire/codec"
	"github.com/segdumping/leanwire/server"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func writeMsg(t *testing.T, w io.Writer, m codec.Msg) {
	t.Helper()
	if err := codec.DefaultCodec(64*1024).WriteMsg(w, m); err != nil {
		t.Fatal(err)
	}
}

// 命中派发、未命中走未知策略、重复注册覆盖并继续工作。
func TestRouteDispatch(t *testing.T) {
	testServer := server.New(server.WithLogger(quietLogger()))
	defer func() { _ = testServer.Shutdown(nil) }()
	routeTable := Bind(testServer, WithLogger(quietLogger()))

	var mu sync.Mutex
	var got []string
	routeTable.Handle(1, func(*server.Conn, codec.Msg) {
		mu.Lock()
		got = append(got, "one")
		mu.Unlock()
	})
	routeTable.Handle(1, func(*server.Conn, codec.Msg) { // 重复：覆盖并记 Warn
		mu.Lock()
		got = append(got, "one-v2")
		mu.Unlock()
	})
	routeTable.Handle(2, func(*server.Conn, codec.Msg) {
		mu.Lock()
		got = append(got, "two")
		mu.Unlock()
	})
	if !routeTable.Registered(1) || !routeTable.Registered(2) || routeTable.Registered(3) {
		t.Fatal("Registered probe wrong")
	}

	serverEnd, testEnd := net.Pipe()
	defer testEnd.Close()
	if _, err := testServer.Connect(serverEnd); err != nil {
		t.Fatal(err)
	}
	c := codec.DefaultCodec(64 * 1024)
	// 命中 + 未命中 + 命中：未知帧不中断连接
	for _, id := range []uint32{1, 99, 2} {
		if err := c.WriteMsg(testEnd, codec.Msg{ID: id, Body: []byte("x")}); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		mu.Lock()
		n := len(got)
		mu.Unlock()
		if n >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("dispatch stalled: got %v", got)
		}
		time.Sleep(2 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 2 || got[0] != "one-v2" || got[1] != "two" {
		t.Fatalf("dispatch = %v", got)
	}
}

// 未知帧自定义策略生效。
func TestUnknownPolicy(t *testing.T) {
	testServer := server.New(server.WithLogger(quietLogger()))
	defer func() { _ = testServer.Shutdown(nil) }()

	var hit atomic.Int64
	routeTable := Bind(testServer, WithLogger(quietLogger()), WithUnknown(func(_ *server.Conn, m codec.Msg) {
		hit.Add(1)
	}))
	routeTable.Handle(1, func(*server.Conn, codec.Msg) {})

	serverEnd, testEnd := net.Pipe()
	defer testEnd.Close()
	if _, err := testServer.Connect(serverEnd); err != nil {
		t.Fatal(err)
	}
	writeMsg(t, testEnd, codec.Msg{ID: 77, Body: []byte("unknown")})
	writeMsg(t, testEnd, codec.Msg{ID: 1, Body: []byte("known")})

	deadline := time.Now().Add(2 * time.Second)
	for hit.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("unknown policy not invoked")
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// MustHandle / nil / started 冻结纪律。
func TestMustHandleDiscipline(t *testing.T) {
	testServer := server.New(server.WithLogger(quietLogger()))
	defer func() { _ = testServer.Shutdown(nil) }()
	routeTable := Bind(testServer, WithLogger(quietLogger()))

	routeTable.MustHandle(1, func(*server.Conn, codec.Msg) {})
	panicked := func(f func()) bool {
		p := false
		func() {
			defer func() { p = recover() != nil }()
			f()
		}()
		return p
	}
	if !panicked(func() { routeTable.MustHandle(1, nil) }) {
		t.Fatal("nil MustHandle should panic")
	}
	if !panicked(func() { routeTable.MustHandle(1, func(*server.Conn, codec.Msg) {}) }) {
		t.Fatal("duplicate MustHandle should panic")
	}

	serverEnd, _ := net.Pipe()
	session, err := testServer.Connect(serverEnd)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = serverEnd.Close() }()
	_ = session // started
	if !panicked(func() { routeTable.Handle(2, func(*server.Conn, codec.Msg) {}) }) {
		t.Fatal("Handle after Serve/Connect should panic")
	}
	if !panicked(func() { routeTable.MustHandle(3, func(*server.Conn, codec.Msg) {}) }) {
		t.Fatal("MustHandle after Serve/Connect should panic")
	}
}

// 无未知策略缺省丢弃（不 panic）：发一帧未注册 ID 后连接可用。
func TestDefaultUnknownDrop(t *testing.T) {
	testServer := server.New(server.WithLogger(quietLogger()))
	defer func() { _ = testServer.Shutdown(nil) }()
	routeTable := Bind(testServer, WithLogger(quietLogger()))
	routeTable.Handle(1, func(s *server.Conn, m codec.Msg) { _ = s.Send(m) })

	serverEnd, testEnd := net.Pipe()
	defer testEnd.Close()
	if _, err := testServer.Connect(serverEnd); err != nil {
		t.Fatal(err)
	}
	writeMsg(t, testEnd, codec.Msg{ID: 99, Body: []byte("drop-me")})
	writeMsg(t, testEnd, codec.Msg{ID: 1, Body: []byte("echo")})

	c := codec.DefaultCodec(64 * 1024)
	_ = testEnd.SetReadDeadline(time.Now().Add(2 * time.Second))
	m, err := c.ReadMsg(testEnd)
	if err != nil {
		t.Fatalf("echo: %v", err)
	}
	if m.ID != 1 || !bytes.Equal(m.Body, []byte("echo")) {
		t.Fatalf("echo mismatch: %+v", m)
	}
}

// Bind 之后不能二次 Bind（SetRootHandler 冻结：根只许一个所有者）。
func TestBindOnce(t *testing.T) {
	testServer := server.New(server.WithLogger(quietLogger()))
	defer func() { _ = testServer.Shutdown(nil) }()
	_ = Bind(testServer, WithLogger(quietLogger()))

	panicked := func(f func()) bool {
		p := false
		func() {
			defer func() { p = recover() != nil }()
			f()
		}()
		return p
	}
	if !panicked(func() { Bind(testServer) }) {
		t.Fatal("second Bind should panic (root frozen)")
	}
}
