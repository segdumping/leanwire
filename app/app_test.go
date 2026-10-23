package app

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/segdumping/leanwire/codec"
	grpctran "github.com/segdumping/leanwire/ext/grpc"
	"github.com/segdumping/leanwire/ext/heartbeat"
	"github.com/segdumping/leanwire/ext/registry"
	"github.com/segdumping/leanwire/ext/rpc"
	"github.com/segdumping/leanwire/server"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// newServerApp 建一个带 TCP listener 的 app（listener 已接入，未 Run）。
func newServerApp(t *testing.T, name string, opts ...Option) (*App, string) {
	t.Helper()
	opts = append([]Option{WithLogger(quietLogger())}, opts...)
	a := New(name, opts...)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	a.Serve(listener)
	t.Cleanup(func() {
		a.Exit()
		if a.started.Load() {
			select {
			case <-a.Done():
			case <-time.After(5 * time.Second):
			}
		}
	})
	return a, listener.Addr().String()
}

// runApp 后台 Run；停止由调用方 Exit + Done 观测。
func runApp(a *App) {
	go func() { _ = a.Run() }()
}

// waitDone 限时等待停止完成。
func waitDone(t *testing.T, a *App) {
	t.Helper()
	select {
	case <-a.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("app not stopped in time")
	}
}

// waitFor 限时轮询断言条件。
func waitFor(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met in time")
}

// callOnce 单次请求，每次自带 500ms 超时（轮询用，不共享外层 ctx）。
func callOnce(service *Client, id uint32, body []byte) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	return service.Call(ctx, id, body)
}

func ctxTimeout(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

// handleRPC 注册一个只关心 body 的测试请求 handler，屏蔽 Conn/msgID。
func handleRPC(a *App, id uint32, h func(body []byte, reply rpc.Reply)) {
	a.RPC().Handle(id, func(_ *server.Conn, _ uint32, body []byte, reply rpc.Reply) {
		h(body, reply)
	})
}

// 端到端：目录注册/发现、WaitReady、请求/响应、并发请求、单向回显、
// 对端退出 → 连接断开 + 后续请求快速失败、双方干净停止。
func TestEndToEnd(t *testing.T) {
	reg := registry.NewMemory()
	defer reg.Close()

	serverApp, _ := newServerApp(t, "service", WithRegistry(reg))
	serverApp.Handle(1, func(s Session, body []byte) { _ = s.SendWaitBytes(context.Background(), 1, body) })
	handleRPC(serverApp, 2, func(body []byte, reply rpc.Reply) {
		_ = reply(append([]byte("pong:"), body...), nil)
	})
	runApp(serverApp)

	clientApp := New("client", WithLogger(quietLogger()), WithRegistry(reg))
	service := clientApp.Depend("service", WithBackoff(20*time.Millisecond, 200*time.Millisecond))
	recv := make(chan string, 1)
	clientApp.Handle(1, func(s Session, body []byte) { recv <- string(body) })
	runApp(clientApp)

	ctx := ctxTimeout(t, 2*time.Second)
	if err := service.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}

	resp, err := service.Call(ctx, 2, []byte("ping"))
	if err != nil {
		t.Fatalf("Call: %v", err)
	}
	if string(resp) != "pong:ping" {
		t.Fatalf("resp = %q", resp)
	}

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			payload := []byte{'a' + byte(i)}
			r, err := service.Call(ctx, 2, payload)
			if err != nil {
				t.Errorf("req %d: %v", i, err)
				return
			}
			if r[len(r)-1] != payload[0] {
				t.Errorf("req %d: resp %q", i, r)
			}
		}(i)
	}
	wg.Wait()

	if err := service.Send(ctx, 1, []byte("hello")); err != nil {
		t.Fatalf("Send: %v", err)
	}
	select {
	case got := <-recv:
		if got != "hello" {
			t.Fatalf("echo = %q", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no echo")
	}

	serverApp.Exit()
	waitDone(t, serverApp)
	waitFor(t, 2*time.Second, func() bool { return !service.Available() })
	if _, err := service.Call(ctx, 2, []byte("x")); !errors.Is(err, ErrNotConnected) {
		t.Fatalf("after down: err = %v", err)
	}

	clientApp.Exit()
	waitDone(t, clientApp)
}

// 静态地址（无目录）依赖：WithAddress 直连可用。
func TestStaticAddress(t *testing.T) {
	serverApp, addr := newServerApp(t, "service")
	handleRPC(serverApp, 7, func(body []byte, reply rpc.Reply) { _ = reply([]byte("P:"+string(body)), nil) })
	runApp(serverApp)

	clientApp := New("client", WithLogger(quietLogger()))
	service := clientApp.Depend("service", WithAddress(addr), WithBackoff(20*time.Millisecond, 200*time.Millisecond))
	runApp(clientApp)

	ctx := ctxTimeout(t, 2*time.Second)
	if err := service.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}
	resp, err := service.Call(ctx, 7, []byte("x"))
	if err != nil || string(resp) != "P:x" {
		t.Fatalf("resp = %q, %v", resp, err)
	}
	clientApp.Exit()
	waitDone(t, clientApp)
}

// 异步请求：发送后立即返回，响应在回调中处理。
func TestClientCallAsync(t *testing.T) {
	serverApp, addr := newServerApp(t, "service")
	handleRPC(serverApp, 8, func(body []byte, reply rpc.Reply) {
		_ = reply(append([]byte("async:"), body...), nil)
	})
	runApp(serverApp)

	clientApp := New("client", WithLogger(quietLogger()))
	service := clientApp.Depend("service", WithAddress(addr), WithBackoff(20*time.Millisecond, 200*time.Millisecond))
	runApp(clientApp)
	ctx := ctxTimeout(t, 2*time.Second)
	if err := service.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}

	type asyncResult struct {
		body []byte
		err  error
	}
	done := make(chan asyncResult, 1)
	if err := service.CallAsync(ctx, 8, []byte("ping"), func(body []byte, err error) {
		done <- asyncResult{body: body, err: err}
	}); err != nil {
		t.Fatalf("CallAsync: %v", err)
	}
	select {
	case r := <-done:
		if r.err != nil || string(r.body) != "async:ping" {
			t.Fatalf("async resp = %q, err = %v", r.body, r.err)
		}
	case <-ctx.Done():
		t.Fatal("async callback not called")
	}
	clientApp.Exit()
	waitDone(t, clientApp)
	serverApp.Exit()
	waitDone(t, serverApp)
}

// 无目录且无静态地址：broken 依赖所有调用即时报错。
func TestNoDirectory(t *testing.T) {
	a := New("x", WithLogger(quietLogger()))
	service := a.Depend("nope")
	if !errors.Is(service.WaitReady(context.Background()), ErrNoDirectory) {
		t.Fatal("want ErrNoDirectory from WaitReady")
	}
	if err := service.Send(context.Background(), 1, nil); !errors.Is(err, ErrNoDirectory) {
		t.Fatalf("Send err = %v", err)
	}
	if _, err := service.Call(context.Background(), 1, nil); !errors.Is(err, ErrNoDirectory) {
		t.Fatalf("Call err = %v", err)
	}
}

// 请求无 handler：不回复，调用方按 ctx 超时（框架不设默认超时）。
func TestCallTimeout(t *testing.T) {
	serverApp, addr := newServerApp(t, "service") // 未注册 RPC handler(2)
	runApp(serverApp)

	clientApp := New("client", WithLogger(quietLogger()))
	service := clientApp.Depend("service", WithAddress(addr), WithBackoff(20*time.Millisecond, 200*time.Millisecond))
	runApp(clientApp)

	ctx := ctxTimeout(t, 2*time.Second)
	if err := service.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}
	timeoutContext, timeoutCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer timeoutCancel()
	if _, err := service.Call(timeoutContext, 2, []byte("x")); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want DeadlineExceeded", err)
	}
	clientApp.Exit()
	waitDone(t, clientApp)
}

// 错误码：对端回 rpc.RemoteError 的 code/msg 原样到达请求方。
func TestRemoteErrorCode(t *testing.T) {
	serverApp, addr := newServerApp(t, "service")
	handleRPC(serverApp, 2, func(_ []byte, reply rpc.Reply) {
		_ = reply(nil, &rpc.RemoteError{Code: 42, Msg: "no such uid"})
	})
	runApp(serverApp)

	clientApp := New("client", WithLogger(quietLogger()))
	service := clientApp.Depend("service", WithAddress(addr), WithBackoff(20*time.Millisecond, 200*time.Millisecond))
	runApp(clientApp)

	ctx := ctxTimeout(t, 2*time.Second)
	if err := service.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}
	_, err := service.Call(ctx, 2, []byte("x"))
	var e *rpc.RemoteError
	if !errors.As(err, &e) || e.Code != 42 || e.Msg != "no such uid" {
		t.Fatalf("err = %v, want *rpc.RemoteError{42, no such uid}", err)
	}
	clientApp.Exit()
	waitDone(t, clientApp)
	serverApp.Exit()
	waitDone(t, serverApp)
}

// 故障转移：同名两实例，client 的单条连接连上其一；该实例停止后，连接经目录
// 快照切到另一实例，请求继续成功。
func TestFailover(t *testing.T) {
	reg := registry.NewMemory()
	defer reg.Close()

	mk := func(tag string) *App {
		a, _ := newServerApp(t, "service", WithRegistry(reg))
		handleRPC(a, 2, func(_ []byte, reply rpc.Reply) { _ = reply([]byte(tag), nil) })
		runApp(a)
		return a
	}
	a1 := mk("n1")
	a2 := mk("n2")

	clientApp := New("client", WithLogger(quietLogger()), WithRegistry(reg))
	service := clientApp.Depend("service", WithBackoff(20*time.Millisecond, 100*time.Millisecond))
	runApp(clientApp)

	ctx := ctxTimeout(t, 5*time.Second)
	if err := service.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}
	first, err := service.Call(ctx, 2, []byte("q"))
	if err != nil {
		t.Fatalf("first request: %v", err)
	}
	// 停掉当前所连的那一个：另一实例存活，连接应切过去。
	victim, survivor, survivorTag := a1, a2, "n2"
	if string(first) == "n2" {
		victim, survivor, survivorTag = a2, a1, "n1"
	}
	victim.Exit()
	waitDone(t, victim)
	waitFor(t, 8*time.Second, func() bool {
		r, err := callOnce(service, 2, []byte("q"))
		return err == nil && string(r) == survivorTag
	})

	clientApp.Exit()
	waitDone(t, clientApp)
	survivor.Exit()
	waitDone(t, survivor)
}

// 按地址路由（业务层做法）：每个节点一个依赖，业务自管 uid→节点。
func TestPerAddressRouting(t *testing.T) {
	serverApp1, addr1 := newServerApp(t, "service")
	handleRPC(serverApp1, 2, func(_ []byte, reply rpc.Reply) { _ = reply([]byte("n1"), nil) })
	runApp(serverApp1)
	serverApp2, addr2 := newServerApp(t, "service")
	handleRPC(serverApp2, 2, func(_ []byte, reply rpc.Reply) { _ = reply([]byte("n2"), nil) })
	runApp(serverApp2)

	clientApp := New("client", WithLogger(quietLogger()))
	dep1 := clientApp.Depend("service@"+addr1, WithAddress(addr1), WithBackoff(20*time.Millisecond, 100*time.Millisecond))
	dep2 := clientApp.Depend("service@"+addr2, WithAddress(addr2), WithBackoff(20*time.Millisecond, 100*time.Millisecond))
	runApp(clientApp)

	ctx := ctxTimeout(t, 3*time.Second)
	for _, dep := range []*Client{dep1, dep2} {
		if err := dep.WaitReady(ctx); err != nil {
			t.Fatalf("WaitReady: %v", err)
		}
	}
	if r, err := dep1.Call(ctx, 2, nil); err != nil || string(r) != "n1" {
		t.Fatalf("dep1 = %q, %v", r, err)
	}
	if r, err := dep2.Call(ctx, 2, nil); err != nil || string(r) != "n2" {
		t.Fatalf("dep2 = %q, %v", r, err)
	}
	clientApp.Exit()
	waitDone(t, clientApp)
}

// 运行期 Depend：Run 之后声明的依赖立即开始建立连接。
func TestDependAfterRun(t *testing.T) {
	serverApp, addr := newServerApp(t, "service")
	handleRPC(serverApp, 2, func(_ []byte, reply rpc.Reply) { _ = reply([]byte("late-ok"), nil) })
	runApp(serverApp)

	clientApp := New("client", WithLogger(quietLogger()))
	runApp(clientApp)
	for !clientApp.started.Load() {
		time.Sleep(time.Millisecond)
	}
	service := clientApp.Depend("late", WithAddress(addr), WithBackoff(20*time.Millisecond, 100*time.Millisecond))

	ctx := ctxTimeout(t, 3*time.Second)
	if err := service.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}
	if r, err := service.Call(ctx, 2, nil); err != nil || string(r) != "late-ok" {
		t.Fatalf("resp = %q, %v", r, err)
	}
	clientApp.Exit()
	waitDone(t, clientApp)
}

// 启动序：OnStart 完成之前不会有任何入站回调执行。
func TestOnStartBeforeTraffic(t *testing.T) {
	var ready atomic.Bool
	var early atomic.Int32
	a := New("service", WithLogger(quietLogger()))
	a.OnStart(func(*App) {
		time.Sleep(200 * time.Millisecond)
		ready.Store(true)
	})
	handleRPC(a, 2, func(_ []byte, reply rpc.Reply) {
		if !ready.Load() {
			early.Add(1)
		}
		_ = reply([]byte("ok"), nil)
	})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	a.Serve(listener)
	runApp(a)

	clientApp := New("client", WithLogger(quietLogger()))
	service := clientApp.Depend("service", WithAddress(listener.Addr().String()), WithBackoff(20*time.Millisecond, 100*time.Millisecond))
	runApp(clientApp)

	ctx := ctxTimeout(t, 3*time.Second)
	if err := service.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}
	if r, err := service.Call(ctx, 2, nil); err != nil || string(r) != "ok" {
		t.Fatalf("resp = %q, %v", r, err)
	}
	if early.Load() != 0 {
		t.Fatalf("%d requests handled before OnStart finished", early.Load())
	}
	clientApp.Exit()
	waitDone(t, clientApp)
	a.Exit()
	waitDone(t, a)
}

// 停止序：OnStop 按序 → 内核关闭 → OnFini 逆序。
func TestOnFiniOrder(t *testing.T) {
	a := New("x", WithLogger(quietLogger()))
	var mu sync.Mutex
	var order []string
	rec := func(s string) func(*App) {
		return func(*App) {
			mu.Lock()
			order = append(order, s)
			mu.Unlock()
		}
	}
	a.OnStop(rec("stop1"))
	a.OnStop(rec("stop2"))
	a.OnFini(rec("fini1"))
	a.OnFini(rec("fini2"))
	runApp(a)
	for !a.started.Load() {
		time.Sleep(time.Millisecond)
	}
	a.Exit()
	waitDone(t, a)

	mu.Lock()
	defer mu.Unlock()
	want := []string{"stop1", "stop2", "fini2", "fini1"}
	if len(order) != len(want) {
		t.Fatalf("order = %v, want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("order = %v, want %v", order, want)
		}
	}
}

// 退出期拒收：OnStop 执行期间到达的请求不进入 handler。
func TestExitRejectsCalls(t *testing.T) {
	a, addr := newServerApp(t, "service")
	var handled atomic.Int32
	handleRPC(a, 2, func(_ []byte, reply rpc.Reply) {
		handled.Add(1)
		_ = reply([]byte("ok"), nil)
	})
	entered := make(chan struct{})
	a.OnStop(func(*App) {
		close(entered)
		time.Sleep(200 * time.Millisecond) // 期间会有请求到达
	})
	runApp(a)

	clientApp := New("client", WithLogger(quietLogger()))
	service := clientApp.Depend("service", WithAddress(addr), WithBackoff(20*time.Millisecond, 100*time.Millisecond))
	runApp(clientApp)
	ctx := ctxTimeout(t, 2*time.Second)
	if err := service.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}

	a.Exit()
	<-entered
	requestContext, requestCancel := context.WithTimeout(context.Background(), time.Second)
	defer requestCancel()
	if _, err := service.Call(requestContext, 2, nil); err == nil {
		t.Fatal("request during shutdown should not get a reply")
	}
	waitDone(t, a)
	if n := handled.Load(); n != 0 {
		t.Fatalf("handler ran %d times during exit", n)
	}
	clientApp.Exit()
	waitDone(t, clientApp)
}

// gRPC 双向流作为传输接口：服务端经流监听接入，客户端 WithDialFunc 拨流；
// 请求/响应语义不变（rpc.DefaultID 复用通道跑在流上）。
func TestGRPCTransport(t *testing.T) {
	tcp, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gl := grpctran.NewListener(nil)
	gs := ggrpc.NewServer()
	grpctran.Register(gs, gl)
	go func() { _ = gs.Serve(tcp) }()
	defer gs.Stop()

	serverApp := New("service", WithLogger(quietLogger()))
	handleRPC(serverApp, 2, func(body []byte, reply rpc.Reply) { _ = reply([]byte("pong:"+string(body)), nil) })
	serverApp.Serve(gl)
	runApp(serverApp)

	clientApp := New("client", WithLogger(quietLogger()))
	service := clientApp.Depend("service",
		WithAddress(tcp.Addr().String()),
		WithDialFunc(func(ctx context.Context, addr string, timeout time.Duration) (net.Conn, error) {
			// timeout 仅约束握手（计时赛跑）；连接生命周期由自身 Close 管理，
			// ctx 经 WithoutCancel 解耦（避免外部取消杀流）。
			type res struct {
				c   net.Conn
				err error
			}
			done := make(chan res, 1)
			go func() {
				c, err := grpctran.Dial(context.WithoutCancel(ctx), addr,
					ggrpc.WithTransportCredentials(insecure.NewCredentials()))
				done <- res{c, err}
			}()
			timer := time.NewTimer(timeout)
			defer timer.Stop()
			select {
			case r := <-done:
				return r.c, r.err
			case <-timer.C:
				return nil, errors.New("grpc dial timeout")
			}
		}),
		WithBackoff(20*time.Millisecond, 200*time.Millisecond),
	)
	runApp(clientApp)

	ctx := ctxTimeout(t, 3*time.Second)
	if err := service.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}
	resp, err := service.Call(ctx, 2, []byte("ping"))
	if err != nil {
		t.Fatalf("Call: %v", err)
	}
	if string(resp) != "pong:ping" {
		t.Fatalf("resp = %q", resp)
	}
	clientApp.Exit()
	waitDone(t, clientApp)
	serverApp.Exit()
	waitDone(t, serverApp)
}

// 目录注册与摘除：Run 注册 advertise 地址，停止后快照转空。
func TestRegisterAndDereg(t *testing.T) {
	reg := registry.NewMemory()
	defer reg.Close()

	serverApp, addr := newServerApp(t, "service", WithRegistry(reg))
	epsCh := make(chan []registry.Endpoint, 8)
	if err := reg.Watch(context.Background(), "service", func(endpoints []registry.Endpoint) {
		select {
		case epsCh <- endpoints:
		default:
		}
	}); err != nil {
		t.Fatal(err)
	}
	runApp(serverApp)

	deadline := time.After(2 * time.Second)
registered:
	for {
		select {
		case endpoints := <-epsCh:
			for _, endpoint := range endpoints {
				if endpoint.Addr == addr {
					break registered
				}
			}
		case <-deadline:
			t.Fatal("no advertise snapshot")
		}
	}
	serverApp.Exit()
	waitDone(t, serverApp)
	for {
		select {
		case endpoints := <-epsCh:
			if len(endpoints) == 0 {
				return // 摘除已交付
			}
		case <-time.After(2 * time.Second):
			t.Fatal("dereg snapshot not delivered")
		}
	}
}

// 同连接消息严格保序：单连接 100 条回显按序返回（handler 内联执行）。
func TestInboundOrderPerConn(t *testing.T) {
	a, addr := newServerApp(t, "service")
	a.Handle(1, func(s Session, body []byte) { _ = s.SendWaitBytes(context.Background(), 1, body) })
	runApp(a)

	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	cdec := codec.DefaultCodec(64 * 1024)

	const n = 100
	errCh := make(chan error, 1)
	go func() {
		for i := 0; i < n; i++ {
			body := []byte{byte(i >> 24), byte(i >> 16), byte(i >> 8), byte(i)}
			if err := cdec.WriteMsg(c, codec.Msg{ID: 1, Body: body}); err != nil {
				errCh <- err
				return
			}
		}
	}()
	for i := 0; i < n; i++ {
		m, err := cdec.ReadMsg(c)
		if err != nil {
			t.Fatalf("read #%d: %v", i, err)
		}
		want := []byte{byte(i >> 24), byte(i >> 16), byte(i >> 8), byte(i)}
		if m.ID != 1 || string(m.Body) != string(want) {
			t.Fatalf("echo #%d: got %+v, want %s", i, m, want)
		}
	}
	select {
	case err := <-errCh:
		t.Fatalf("writer: %v", err)
	default:
	}
	a.Exit()
	waitDone(t, a)
}

// 生命周期：Run 二次调用报错；装配接口在 Run 后 panic。
func TestLifecycleGuards(t *testing.T) {
	a := New("x", WithLogger(quietLogger()))
	runApp(a)
	for !a.started.Load() {
		time.Sleep(time.Millisecond)
	}
	if err := a.Run(); !errors.Is(err, ErrStarted) {
		t.Fatalf("second Run = %v", err)
	}
	for _, f := range []func(){
		func() { a.Handle(1, func(Session, []byte) {}) },
		func() { a.OnStart(func(*App) {}) },
		func() { a.OnStop(func(*App) {}) },
		func() { a.OnFini(func(*App) {}) },
		func() { _ = a.Listen("127.0.0.1:0") },
	} {
		func() {
			defer func() { _ = recover() }()
			f()
			t.Error("assembly after Run should panic")
		}()
	}
	a.Exit()
	waitDone(t, a)
}

// 心跳与失效连接检测（WithHeartbeat，idle 自动联动 3d）：
//  1. 对端回声 0xF001 → 安静期连接存活（不被空闲超时误杀）；
//  2. 对端静默 → 空闲约 3d 关闭（失效连接快速检测，原 10 分钟）；
//  3. 两端都配置的服务连接：静默期远超空闲窗仍存活（连接保活）。
func TestHeartbeat(t *testing.T) {
	a := New("service", WithLogger(quietLogger()), WithHeartbeat(100*time.Millisecond))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	a.Serve(listener)
	runApp(a)

	// (1) 回声客户端：把 0xF001 原样发回，连接应穿过 3d=300ms 的空闲窗。
	c, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	echoDone := make(chan struct{})
	go func() {
		defer close(echoDone)
		cdec := codec.DefaultCodec(64 * 1024)
		for {
			m, err := cdec.ReadMsg(c)
			if err != nil {
				return
			}
			if m.ID == heartbeat.DefaultID {
				_ = cdec.WriteMsg(c, m)
			}
		}
	}()
	time.Sleep(time.Second)
	select {
	case <-echoDone:
		t.Fatal("echoing session closed despite heartbeat")
	default:
	}

	// (2) 静默客户端：不回声 → 约 300ms 被 idle 关闭（失效连接检测）。
	c2, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	start := time.Now()
	_ = c2.SetReadDeadline(time.Now().Add(2 * time.Second))
	codec2 := codec.DefaultCodec(64 * 1024)
	for {
		if _, err := codec2.ReadMsg(c2); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			t.Fatalf("silent client: err = %v (after %v)", err, time.Since(start))
		}
	}
	if el := time.Since(start); el > time.Second {
		t.Fatalf("dead-link detection too slow: %v", el)
	}

	// (3) 服务连接保活：两端心跳、无业务流量，连接存活远超空闲窗。
	reg := registry.NewMemory()
	defer reg.Close()
	serverApp, _ := newServerApp(t, "service", WithRegistry(reg), WithHeartbeat(100*time.Millisecond))
	runApp(serverApp)

	clientApp := New("client", WithLogger(quietLogger()), WithRegistry(reg), WithHeartbeat(100*time.Millisecond))
	service := clientApp.Depend("service", WithBackoff(20*time.Millisecond, 100*time.Millisecond))
	runApp(clientApp)

	ctx := ctxTimeout(t, 2*time.Second)
	if err := service.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}
	time.Sleep(time.Second)
	if !service.Available() {
		t.Fatal("quiet service link killed by idle")
	}

	clientApp.Exit()
	waitDone(t, clientApp)
	a.Exit()
	waitDone(t, a)
}

// 保留号防护：业务不得占用 RPC 通道号与心跳号。
func TestReservedID(t *testing.T) {
	a := New("x", WithLogger(quietLogger()))
	for _, id := range []uint32{rpc.DefaultID, heartbeat.DefaultID} {
		panicked := false
		func() {
			defer func() { panicked = recover() != nil }()
			a.Handle(id, func(Session, []byte) {})
		}()
		if !panicked {
			t.Fatalf("Handle(%#x) should panic", id)
		}
	}

	service := a.Depend("y", WithAddress("127.0.0.1:1"))
	ctx := context.Background()
	for _, id := range []uint32{rpc.DefaultID, heartbeat.DefaultID} {
		if err := service.Send(ctx, id, nil); !errors.Is(err, ErrReservedID) {
			t.Fatalf("Send(%#x) err = %v", id, err)
		}
		if _, err := service.Call(ctx, id, nil); !errors.Is(err, ErrReservedID) {
			t.Fatalf("Call(%#x) err = %v", id, err)
		}
	}
}

// 多服务：同一 app 依赖两个服务，各自一条连接，请求各达其位。
func TestMultiService(t *testing.T) {
	reg := registry.NewMemory()
	defer reg.Close()

	mk := func(name, tag string) {
		a, _ := newServerApp(t, name, WithRegistry(reg))
		handleRPC(a, 2, func(body []byte, reply rpc.Reply) { _ = reply([]byte(tag+":"+string(body)), nil) })
		runApp(a)
	}
	mk("alpha", "a1")
	mk("beta", "b1")

	clientApp := New("client", WithLogger(quietLogger()), WithRegistry(reg))
	depA := clientApp.Depend("alpha", WithBackoff(20*time.Millisecond, 100*time.Millisecond))
	depB := clientApp.Depend("beta", WithBackoff(20*time.Millisecond, 100*time.Millisecond))
	runApp(clientApp)

	ctx := ctxTimeout(t, 5*time.Second)
	if err := depA.WaitReady(ctx); err != nil {
		t.Fatalf("alpha WaitReady: %v", err)
	}
	if err := depB.WaitReady(ctx); err != nil {
		t.Fatalf("beta WaitReady: %v", err)
	}
	if r, err := depA.Call(ctx, 2, []byte("p")); err != nil || string(r) != "a1:p" {
		t.Fatalf("alpha = %q, %v", r, err)
	}
	if r, err := depB.Call(ctx, 2, []byte("p")); err != nil || string(r) != "b1:p" {
		t.Fatalf("beta = %q, %v", r, err)
	}
	clientApp.Exit()
	waitDone(t, clientApp)
}

// 连接恢复：对端停止后按退避重拨，同址重启后连接自动恢复。
// 注意：lb 的失败冷却（默认 3s）会延后重拨，因此等待窗口取 6s。
func TestLinkReconnect(t *testing.T) {
	ln1, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln1.Addr().String()

	serverApp := New("service", WithLogger(quietLogger()))
	handleRPC(serverApp, 2, func(body []byte, reply rpc.Reply) { _ = reply([]byte("v1:"+string(body)), nil) })
	serverApp.Serve(ln1)
	runApp(serverApp)

	clientApp := New("client", WithLogger(quietLogger()))
	service := clientApp.Depend("service", WithAddress(addr), WithBackoff(20*time.Millisecond, 100*time.Millisecond))
	runApp(clientApp)

	ctx := ctxTimeout(t, 3*time.Second)
	if err := service.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}
	if r, err := service.Call(ctx, 2, []byte("q")); err != nil || string(r) != "v1:q" {
		t.Fatalf("resp = %q, %v", r, err)
	}

	serverApp.Exit()
	waitDone(t, serverApp)
	waitFor(t, 2*time.Second, func() bool { return !service.Available() })

	var ln2 net.Listener
	for i := 0; i < 50; i++ {
		if ln2, err = net.Listen("tcp", addr); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("relisten %s: %v", addr, err)
	}
	serverApp2 := New("service", WithLogger(quietLogger()))
	handleRPC(serverApp2, 2, func(body []byte, reply rpc.Reply) { _ = reply([]byte("v2:"+string(body)), nil) })
	serverApp2.Serve(ln2)
	runApp(serverApp2)

	waitFor(t, 6*time.Second, func() bool {
		r, err := callOnce(service, 2, []byte("q"))
		return err == nil && string(r) == "v2:q"
	})

	clientApp.Exit()
	waitDone(t, clientApp)
	serverApp2.Exit()
	waitDone(t, serverApp2)
}

// MustHandle / RPC MustHandle：重复注册与 nil handler 一律 panic。
func TestAppMustHandlePanics(t *testing.T) {
	a := New("m1", WithLogger(quietLogger()))
	mustPanic := func(f func()) bool {
		panicked := false
		func() {
			defer func() { panicked = recover() != nil }()
			f()
		}()
		return panicked
	}

	a.MustHandle(1, func(Session, []byte) {})
	if !mustPanic(func() { a.MustHandle(1, func(Session, []byte) {}) }) {
		t.Fatal("duplicate MustHandle should panic")
	}
	if !mustPanic(func() { a.MustHandle(heartbeat.DefaultID, func(Session, []byte) {}) }) {
		t.Fatal("reserved-id MustHandle should panic")
	}
	if !mustPanic(func() { a.MustHandle(2, nil) }) {
		t.Fatal("nil MustHandle should panic")
	}

	h := func(*server.Conn, uint32, []byte, rpc.Reply) {}
	a.RPC().MustHandle(2, h)
	if !mustPanic(func() { a.RPC().MustHandle(2, h) }) {
		t.Fatal("duplicate RPC MustHandle should panic")
	}
	if !mustPanic(func() { a.RPC().MustHandle(3, nil) }) {
		t.Fatal("nil RPC MustHandle should panic")
	}

	// 宽松版不受影响。
	a.Handle(3, func(Session, []byte) {})
	a.RPC().Handle(4, h)
}
