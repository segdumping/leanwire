package grpc

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/segdumping/leanwire/codec"
	"github.com/segdumping/leanwire/ext/router"
	"github.com/segdumping/leanwire/server"
)

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// startServer 组装 TCP + gRPC server + 接受面；stop 整体收尾（幂等）。
func startServer(t *testing.T) (net.Listener, func()) {
	t.Helper()
	raw, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gl := NewListener(raw.Addr())
	gs := ggrpc.NewServer()
	Register(gs, gl)
	go func() { _ = gs.Serve(raw) }()
	var once sync.Once
	return gl, func() {
		once.Do(func() {
			_ = gl.Close()
			_ = raw.Close()
			go gs.GracefulStop()
		})
	}
}

// 环回 gRPC 流上跑内核 echo，验证收发顺序与 OnOpen/OnClose 各一次。
func TestGRPCParity(t *testing.T) {
	openCh := make(chan *server.Conn, 1)
	reasonCh := make(chan error, 1)
	routeTable := router.New(router.WithLogger(quiet()))
	a := server.New(server.WithLogger(quiet()), server.WithIdleTimeout(time.Minute),
		server.WithRootHandler(routeTable.Handler()),
		server.WithHooks(server.Hooks{
			OnOpen: func(s *server.Conn) {
				select {
				case openCh <- s:
				default:
				}
			},
			OnClose: func(_ *server.Conn, err error) {
				select {
				case reasonCh <- err:
				default:
				}
			},
		}))
	routeTable.Handle(1, func(s *server.Conn, m codec.Msg) { _ = s.Send(nil, m) })

	listener, stop := startServer(t)
	a.Serve(listener)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := Dial(ctx, listener.Addr().String(), ggrpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}

	c := codec.DefaultCodec(64 * 1024)
	const n = 20
	for i := 0; i < n; i++ {
		if err := c.WriteMsg(client, codec.Msg{ID: 1, Body: []byte{byte(i)}}); err != nil {
			t.Fatalf("send #%d: %v", i, err)
		}
	}
	_ = client.SetReadDeadline(time.Now().Add(5 * time.Second))
	for i := 0; i < n; i++ {
		m, err := c.ReadMsg(client)
		if err != nil {
			t.Fatalf("recv #%d: %v", i, err)
		}
		if len(m.Body) != 1 || m.Body[0] != byte(i) {
			t.Fatalf("out of order at #%d: %v", i, m.Body)
		}
	}

	select {
	case <-openCh:
	default:
		t.Fatal("OnOpen not called")
	}

	// 客户端关闭后，服务端连接收尾且原因非 nil
	_ = client.Close()
	select {
	case err := <-reasonCh:
		if err == nil {
			t.Fatal("close reason must be non-nil")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("OnClose not called")
	}

	sctx, scancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer scancel()
	if err := a.Shutdown(sctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	stop()
}

// 空闲超时经 SetReadDeadline 生效，关闭原因为 ErrIdle。
// 并发 Write 必须串行成帧：每个 net.Conn.Write 独占一次 SendMsg，
// 不出现 grpc "concurrent send" 错误/交织帧。
func TestGRPCConcurrentWrites(t *testing.T) {
	msgCh := make(chan codec.Msg, 64)
	routeTable := router.New(router.WithLogger(quiet()))
	a := server.New(server.WithLogger(quiet()), server.WithIdleTimeout(time.Minute),
		server.WithRootHandler(routeTable.Handler()))
	routeTable.Handle(1, func(_ *server.Conn, m codec.Msg) {
		select {
		case msgCh <- m:
		default:
		}
	})
	listener, stop := startServer(t)
	a.Serve(listener)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := Dial(ctx, listener.Addr().String(), ggrpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	c := codec.DefaultCodec(64 * 1024)
	const n = 32
	var wg sync.WaitGroup
	errCh := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// 整帧一次 Write：并发 WriteMsg 会拆成两次 Write，不属于传输层契约。
			var buffer bytes.Buffer
			if err := c.WriteMsg(&buffer, codec.Msg{ID: 1, Body: []byte{byte(i)}}); err != nil {
				errCh <- err
				return
			}
			if _, err := client.Write(buffer.Bytes()); err != nil {
				errCh <- err
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent write: %v", err)
	}

	got := make(map[byte]bool, n)
	deadline := time.After(5 * time.Second)
	for len(got) < n {
		select {
		case m := <-msgCh:
			if len(m.Body) != 1 {
				t.Fatalf("corrupted frame: %+v", m)
			}
			got[m.Body[0]] = true
		case <-deadline:
			t.Fatalf("got %d/%d frames", len(got), n)
		}
	}

	sctx, scancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer scancel()
	if err := a.Shutdown(sctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	stop()
}

func TestGRPCIdleTimeout(t *testing.T) {
	reasonCh := make(chan error, 1)
	routeTable := router.New(router.WithLogger(quiet()))
	a := server.New(server.WithLogger(quiet()), server.WithIdleTimeout(200*time.Millisecond),
		server.WithRootHandler(routeTable.Handler()),
		server.WithHooks(server.Hooks{
			OnClose: func(_ *server.Conn, err error) { reasonCh <- err },
		}))
	routeTable.Handle(1, func(*server.Conn, codec.Msg) {})
	listener, stop := startServer(t)
	a.Serve(listener)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := Dial(ctx, listener.Addr().String(), ggrpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	select {
	case err := <-reasonCh:
		if err != server.ErrIdle {
			t.Fatalf("reason = %v, want ErrIdle", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("idle close did not happen")
	}

	// 服务端收尾后客户端读应尽快失败
	_ = client.SetReadDeadline(time.Now().Add(2 * time.Second))
	var b [1]byte
	if _, err := client.Read(b[:]); err == nil {
		t.Fatal("client read after server close must fail")
	}

	sctx, scancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer scancel()
	if err := a.Shutdown(sctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	stop()
}

// 半开对端：写侧在 flushTimeout 内报废连接并收尾连接。
func TestGRPCHalfOpenWriteDeadline(t *testing.T) {
	openCh := make(chan *server.Conn, 1)
	reasonCh := make(chan error, 1)
	a := server.New(
		server.WithLogger(quiet()),
		server.WithCodec(codec.DefaultCodec(16<<20)),
		server.WithFlushTimeout(150*time.Millisecond),
		server.WithIdleTimeout(time.Minute),
		server.WithHooks(server.Hooks{
			OnOpen: func(s *server.Conn) {
				select {
				case openCh <- s:
				default:
				}
			},
			OnClose: func(_ *server.Conn, err error) {
				select {
				case reasonCh <- err:
				default:
				}
			},
		}))
	listener, stop := startServer(t)
	a.Serve(listener)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := Dial(ctx, listener.Addr().String(), ggrpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close() // 对端不读：pump 只缓一帧，窗口很快耗尽

	var s *server.Conn
	select {
	case s = <-openCh:
	case <-time.After(2 * time.Second):
		t.Fatal("server session not opened")
	}
	if err := s.Send(nil, codec.Msg{ID: 1, Body: make([]byte, 8<<20)}); err != nil {
		t.Fatalf("send: %v", err)
	}

	select {
	case err := <-reasonCh:
		if err == nil {
			t.Fatal("reason must be non-nil")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("half-open write deadline did not fire")
	}
	if err := s.Send(nil, codec.Msg{ID: 1}); err != server.ErrClosed {
		t.Fatalf("send after write-timeout close: want ErrClosed, got %v", err)
	}

	sctx, scancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer scancel()
	if err := a.Shutdown(sctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	stop()
}

// 写超时可重试：过期 deadline 返回超时错误但不封连接不记 lastErr，
// 恢复 deadline 后连接仍可用（对齐 net.Conn deadline 语义）。
func TestGRPCWriteDeadlineRetry(t *testing.T) {
	msgCh := make(chan codec.Msg, 8)
	routeTable := router.New(router.WithLogger(quiet()))
	a := server.New(server.WithLogger(quiet()), server.WithIdleTimeout(time.Minute),
		server.WithRootHandler(routeTable.Handler()))
	routeTable.Handle(1, func(_ *server.Conn, m codec.Msg) {
		msgCh <- m
	})
	listener, stop := startServer(t)
	a.Serve(listener)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := Dial(ctx, listener.Addr().String(), ggrpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// 写超时：返回超时错误，但连接不关闭
	c := codec.DefaultCodec(64 * 1024)
	frame := func(body string) []byte {
		var b bytes.Buffer
		if err := c.WriteMsg(&b, codec.Msg{ID: 1, Body: []byte(body)}); err != nil {
			t.Fatal(err)
		}
		return b.Bytes()
	}
	if err := client.SetWriteDeadline(time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Write(frame("late")); err != (deadlineError{}) {
		t.Fatalf("err = %v, want deadlineError", err)
	}

	// 恢复 deadline：重试成功
	if err := client.SetWriteDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Write(frame("ok")); err != nil {
		t.Fatalf("retry after deadline: %v", err)
	}

	// 流仍存活：服务端应收到消息
	select {
	case m := <-msgCh:
		if string(m.Body) != "ok" {
			t.Fatalf("body = %q", m.Body)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("stream broken by write deadline")
	}

	sctx, scancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer scancel()
	if err := a.Shutdown(sctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	stop()
}

// 阻塞流：SendMsg 卡在 release 上模拟对端流控停滞，RecvMsg 供 pump 挂起。
type stuckStream struct{ release chan struct{} }

func (s *stuckStream) SendMsg(any) error { <-s.release; return nil }
func (s *stuckStream) RecvMsg(any) error { <-s.release; return io.EOF }

// 写超时后重试：在途 SendMsg 卡住仍持槽位时，重试的 Write 必须受 deadline
// 约束到点返回超时，而不是无限阻塞在槽位等待上。
func TestGRPCWriteDeadlineSlotWait(t *testing.T) {
	release := make(chan struct{})
	c := newConn(&stuckStream{release: release}, nil, nil)
	t.Cleanup(func() {
		close(release) // 解开在途 SendMsg 与 pump
		_ = c.Close()
	})

	// 首次写（无 deadline）：SendMsg 进入在途并卡住，持有槽位
	go func() { _, _ = c.Write([]byte("first")) }()
	deadline := time.Now().Add(2 * time.Second)
	for len(c.sendSlot) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("in-flight send never acquired the slot")
		}
		time.Sleep(time.Millisecond)
	}

	// 重试写：带 deadline，必须到点返回超时而不是永远等槽
	if err := c.SetWriteDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := c.Write([]byte("second"))
		done <- err
	}()
	select {
	case err := <-done:
		if err != (deadlineError{}) {
			t.Fatalf("retry write err = %v, want deadlineError", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("retry write blocked past deadline: slot wait ignores write deadline")
	}
}

// 拨号 ctx 独立性：流建立后取消拨号 ctx 不应随断连接（外部短超时 ctx 拨号用法）。
func TestGRPCDialCtxIndependent(t *testing.T) {
	routeTable := router.New(router.WithLogger(quiet()))
	a := server.New(server.WithLogger(quiet()), server.WithIdleTimeout(time.Minute),
		server.WithRootHandler(routeTable.Handler()))
	routeTable.Handle(1, func(s *server.Conn, m codec.Msg) { _ = s.Send(nil, m) })
	listener, stop := startServer(t)
	a.Serve(listener)

	dialCtx, dialCancel := context.WithTimeout(context.Background(), 2*time.Second)
	client, err := Dial(dialCtx, listener.Addr().String(), ggrpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		dialCancel()
		t.Fatal(err)
	}
	dialCancel() // 立即取消拨号 ctx：已建立的流必须继续存活
	defer client.Close()

	c := codec.DefaultCodec(64 * 1024)
	var buffer bytes.Buffer
	if err := c.WriteMsg(&buffer, codec.Msg{ID: 1, Body: []byte("ping")}); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := client.Write(buffer.Bytes()); err != nil {
		t.Fatalf("write after dial ctx cancel: %v", err)
	}

	_ = client.SetReadDeadline(time.Now().Add(3 * time.Second))
	m, err := c.ReadMsg(client)
	if err != nil || string(m.Body) != "ping" {
		t.Fatalf("read: %v, %q", err, m.Body)
	}

	sctx, scancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer scancel()
	if err := a.Shutdown(sctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	stop()
}

// accept 队列满：有界阻塞等待，超时返回 false（调用方回 ResourceExhausted）；关闭后立即拒绝。
func TestListenerAdoptQueueFull(t *testing.T) {
	saved := adoptWait
	adoptWait = 50 * time.Millisecond
	defer func() { adoptWait = saved }()

	l := NewListener(nil)
	defer func() { _ = l.Close() }()
	var cleanup []func()
	defer func() {
		for _, fn := range cleanup {
			fn()
		}
	}()
	for i := 0; i < acceptQueue; i++ {
		aEnd, bEnd := net.Pipe()
		cleanup = append(cleanup, func() { _ = aEnd.Close(); _ = bEnd.Close() })
		if !l.adoptConn(aEnd) {
			t.Fatalf("conn #%d rejected before queue full", i)
		}
	}
	aEnd, bEnd := net.Pipe()
	defer func() { _ = aEnd.Close(); _ = bEnd.Close() }()
	if l.adoptConn(aEnd) {
		t.Fatal("queue-full adopt should block and time out, not enqueue")
	}
	// 关闭后必须立即拒绝
	_ = l.Close()
	start := time.Now()
	if l.adoptConn(aEnd) {
		t.Fatal("after close must reject")
	}
	if d := time.Since(start); d > 100*time.Millisecond {
		t.Fatalf("close did not unblock promptly: %v", d)
	}
}
