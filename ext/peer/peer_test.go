package peer

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/segdumping/leanwire/server"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestPeerReconnects 依次验证拨号失败、建连接、对端关闭、再失败与 Stop。
func TestPeerReconnects(t *testing.T) {
	app := server.New(server.WithLogger(quietLogger()))

	connected := make(chan *server.Conn, 8)
	failed := make(chan error, 16)
	opened := make(chan *server.Conn, 8) // 证明用户钩子未被 peer 覆盖
	app.AppendHooks(server.Hooks{OnOpen: func(s *server.Conn) { opened <- s }})

	testEndCh := make(chan net.Conn, 8)
	calls := 0
	dialer := func() (net.Conn, error) {
		switch calls++; calls {
		case 1:
			return nil, errors.New("dial fail #1")
		case 2:
			s, c := net.Pipe()
			testEndCh <- c
			return s, nil
		case 3:
			return nil, errors.New("dial fail #2")
		default:
			return nil, errors.New("no more dials expected")
		}
	}

	r := New(app, dialer,
		WithBackoff(10*time.Millisecond, 20*time.Millisecond),
		WithOnConnected(func(s *server.Conn) { connected <- s }),
		WithOnFailed(func(err error) { failed <- err }),
	)
	r.Start()
	defer r.Stop()

	// 第 1 次失败
	select {
	case err := <-failed:
		if err == nil {
			t.Fatal("nil fail reason")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("first dial failure not observed")
	}

	// 第 2 次成功，用户 OnOpen 钩子仍在
	var session *server.Conn
	select {
	case session = <-connected:
		if session == nil {
			t.Fatal("nil session")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("never connected")
	}
	select {
	case <-opened:
	case <-time.After(1 * time.Second):
		t.Fatal("user OnOpen hook lost after peer attach")
	}

	// 对端关闭 → 重拨 → 第 3 次失败
	testEnd := <-testEndCh
	testEnd.Close()
	select {
	case err := <-failed:
		if err.Error() != "dial fail #2" {
			t.Fatalf("unexpected failure: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("missing dial fail #2")
	}
	// Stop 后不再拨号
	r.Stop()
	time.Sleep(60 * time.Millisecond) // 等待循环收尾
	before := calls
	time.Sleep(200 * time.Millisecond)
	if calls != before {
		t.Fatalf("dial continues after Stop: %d -> %d", before, calls)
	}
}

// Start 幂等：重复调用只保留一个拨号循环。
func TestStartIdempotent(t *testing.T) {
	app := server.New(server.WithLogger(quietLogger()))
	defer func() { _ = app.Shutdown(nil) }()

	var dials atomic.Int64
	block := make(chan struct{})
	r := New(app, func() (net.Conn, error) {
		dials.Add(1)
		<-block
		return nil, errors.New("blocked dial")
	}, WithLogger(quietLogger()))

	r.Start()
	r.Start()
	r.Start()

	deadline := time.Now().Add(time.Second)
	for dials.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond) // 给多余的循环足够时间进入 dial
	close(block)
	r.Stop()
	if n := dials.Load(); n != 1 {
		t.Fatalf("Start not idempotent: dial called %d times", n)
	}
}

// 对端"接受即断"时 Done 已就绪，loop 不死等且退避递增防风暴。
func TestPeerRedialsOnImmediateClose(t *testing.T) {
	app := server.New(server.WithLogger(quietLogger()))

	var calls atomic.Int64
	dialer := func() (net.Conn, error) {
		calls.Add(1)
		s, c := net.Pipe()
		_ = c.Close() // 对端接受即断
		return s, nil
	}
	r := New(app, dialer, WithBackoff(10*time.Millisecond, 20*time.Millisecond))
	r.Start()
	defer r.Stop()

	deadline := time.After(2 * time.Second)
	for {
		if calls.Load() >= 3 {
			return // 连续重拨 = 未停摆
		}
		select {
		case <-deadline:
			t.Fatalf("reconnect stalled: dial called only %d times", calls.Load())
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// 重构能力验证：New 不再依赖钩子列表（AppendHooks 冻结窗口），可在 Serve
// 之后创建重连器——动态建立连接的前提。
func TestNewAfterServe(t *testing.T) {
	app := server.New(server.WithLogger(quietLogger()))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	app.Serve(listener) // 先 Serve

	connected := make(chan *server.Conn, 1)
	r := New(app, func() (net.Conn, error) {
		return net.Dial("tcp", listener.Addr().String())
	},
		WithOnConnected(func(s *server.Conn) { connected <- s }),
		WithBackoff(5*time.Millisecond, 20*time.Millisecond),
	)
	r.Start() // Serve 后 New——旧实现在 AppendHooks 处 panic
	defer r.Stop()
	select {
	case <-connected:
	case <-time.After(2 * time.Second):
		t.Fatal("no connection after post-Serve New")
	}
}

func TestPeerStopsWithApp(t *testing.T) {
	app := server.New(server.WithLogger(quietLogger()))

	var calls atomic.Int64
	pending := make(chan struct{}, 1)
	dialer := func() (net.Conn, error) {
		calls.Add(1)
		select {
		case <-pending:
			s, _ := net.Pipe()
			return s, nil
		case <-time.After(3 * time.Second):
			return nil, errors.New("dial timeout")
		}
	}
	r := New(app, dialer, WithBackoff(5*time.Millisecond, 10*time.Millisecond))
	r.Start()

	time.Sleep(30 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := app.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	// 放行一次拨号：Connect 返回 ErrClosed 后循环应退出。
	pending <- struct{}{}
	time.Sleep(300 * time.Millisecond)
	if got := calls.Load(); got != 1 {
		t.Fatalf("dial calls after shutdown = %d, want 1", got)
	}
	r.Stop()
}
