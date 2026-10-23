package app

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/segdumping/leanwire/codec"
	"github.com/segdumping/leanwire/server"
)

// Bind 后可查连接；同 uid 再 Bind 触发顶号。
func gateSession(c *server.Conn) Session { return NewSession(c, c.ID()) }

func TestGateBindKick(t *testing.T) {
	testServer := server.New(server.WithLogger(quietLogger()))
	defer func() { _ = testServer.Shutdown(nil) }()
	g := NewGate(nil, testServer, WithGateLogger(quietLogger()))

	if err := g.Bind(nil, 0); err != ErrInvalidUID {
		t.Fatalf("uid=0: want ErrInvalidUID, got %v", err)
	}

	s1, _ := net.Pipe()
	sess1, err := testServer.Connect(s1)
	if err != nil {
		t.Fatal(err)
	}
	s2, _ := net.Pipe()
	sess2, err := testServer.Connect(s2)
	if err != nil {
		t.Fatal(err)
	}

	if err := g.Bind(gateSession(sess1), 100); err != nil {
		t.Fatal(err)
	}
	target, ok := g.Target(100)
	if !ok || target.Conn() != sess1 {
		t.Fatal("target mismatch after first bind")
	}

	if err := g.Bind(gateSession(sess2), 100); err != nil {
		t.Fatal(err)
	}
	if err := sess1.Send(codec.Msg{ID: 1}); err != server.ErrClosed {
		t.Fatalf("old session Send: want ErrClosed, got %v", err)
	}
	if target, ok := g.Target(100); !ok || target.Conn() != sess2 {
		t.Fatal("second bind lost the uid index")
	}
	g.Unbind(gateSession(sess2))
	if _, ok := g.Target(100); ok {
		t.Fatal("target should be gone after unbind")
	}
}

// 同一连接改绑 uid：旧索引必须被清掉。
func TestGateRebindClearsOldIndex(t *testing.T) {
	testServer := server.New(server.WithLogger(quietLogger()))
	defer func() { _ = testServer.Shutdown(nil) }()
	g := NewGate(nil, testServer)

	serverEnd, testEnd := net.Pipe()
	defer testEnd.Close()
	session, err := testServer.Connect(serverEnd)
	if err != nil {
		t.Fatal(err)
	}

	if err := g.Bind(gateSession(session), 100); err != nil {
		t.Fatal(err)
	}
	if err := g.Bind(gateSession(session), 200); err != nil {
		t.Fatal(err)
	}

	if _, ok := g.Target(100); ok {
		t.Fatal("stale uid 100 still indexed after rebind")
	}
	if tgt, ok := g.Target(200); !ok || tgt.Conn() != session {
		t.Fatal("uid 200 should point at the rebound session")
	}
	g.Unbind(gateSession(session))
	if _, ok := g.Target(200); ok {
		t.Fatal("uid 200 still indexed after unbind")
	}
}

// 转发：Handle 取到 Bind 的 uid 与原始 payload。
func TestGateForward(t *testing.T) {
	testServer := server.New(server.WithLogger(quietLogger()))
	defer func() { _ = testServer.Shutdown(nil) }()
	g := NewGate(nil, testServer)

	type fwd struct {
		sessID  uint64
		uid     uint64
		payload string
	}
	got := make(chan fwd, 4)
	g.Handle(1001, func(s Session, uid uint64, payload []byte) {
		got <- fwd{sessID: s.Conn().ID(), uid: uid, payload: string(payload)}
	})

	c := codec.DefaultCodec(64 * 1024)
	serverEnd, testEnd := net.Pipe()
	session, err := testServer.Connect(serverEnd)
	if err != nil {
		t.Fatal(err)
	}
	go gateDrainUntilClose(c, testEnd)
	if err := session.Send(codec.Msg{ID: 1001, Body: []byte("pre-login")}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-got:
		t.Fatal("forward fired before bind")
	case <-time.After(80 * time.Millisecond):
	}

	if err := g.Bind(gateSession(session), 42); err != nil {
		t.Fatal(err)
	}
	gateWriteMsg(t, testEnd, c, codec.Msg{ID: 1001, Body: []byte("hello")})
	select {
	case f := <-got:
		if f.sessID != session.ID() || f.uid != 42 || f.payload != "hello" {
			t.Fatalf("forward mismatch: %+v", f)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("forward not fired")
	}
}

// Gate 直接注册客户端消息与内部服务消息；客户端 Session 在 Bind 后切到 uid。
func TestGateHandleClientAndServer(t *testing.T) {
	clientServer := server.New(server.WithLogger(quietLogger()))
	defer func() { _ = clientServer.Shutdown(nil) }()

	a := New("gate-handle-test", WithLogger(quietLogger()))
	defer func() { _ = a.Server().Shutdown(nil) }()
	g := NewGate(a, clientServer, WithGateLogger(quietLogger()))

	ids := make(chan SessionID, 4)
	g.HandleClient(1200, func(s Session, body []byte) { ids <- s.ID() })
	g.HandleServer(1201, func(s Session, body []byte) { ids <- s.ID() })

	cx := codec.DefaultCodec(64 * 1024)
	serverEnd, testEnd := net.Pipe()
	defer testEnd.Close()
	conn, err := clientServer.Connect(serverEnd)
	if err != nil {
		t.Fatal(err)
	}
	gateWriteMsg(t, testEnd, cx, codec.Msg{ID: 1200})
	select {
	case id := <-ids:
		if id != conn.ID() {
			t.Fatalf("unbound client session id = %d, want %d", id, conn.ID())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("client handler did not run")
	}

	if err := g.Bind(gateSession(conn), 55); err != nil {
		t.Fatal(err)
	}
	gateWriteMsg(t, testEnd, cx, codec.Msg{ID: 1200})
	select {
	case id := <-ids:
		if id != 55 {
			t.Fatalf("bound client session id = %d, want 55", id)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("bound client handler did not run")
	}

	appEnd, appPeer := net.Pipe()
	defer appPeer.Close()
	appConn, err := a.Server().Connect(appEnd)
	if err != nil {
		t.Fatal(err)
	}
	gateWriteMsg(t, appPeer, cx, codec.Msg{ID: 1201})
	select {
	case id := <-ids:
		if id != appConn.ID() {
			t.Fatalf("server session id = %d, want %d", id, appConn.ID())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server handler did not run")
	}
}

// SendTo / Broadcast 的索引语义与阻塞投递。
func TestGateDeliverHelpers(t *testing.T) {
	testServer := server.New(server.WithLogger(quietLogger()))
	defer func() { _ = testServer.Shutdown(nil) }()
	g := NewGate(nil, testServer, WithGateLogger(quietLogger()))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := g.SendTo(ctx, 999, 1, nil); err != ErrNoConnection {
		t.Fatalf("SendTo unknown uid: want ErrNoConnection, got %v", err)
	}

	sessionServer, testEnd := net.Pipe()
	session, err := testServer.Connect(sessionServer)
	if err != nil {
		t.Fatal(err)
	}
	go gateDrainUntilClose(codec.DefaultCodec(64*1024), testEnd)
	if err := g.Bind(gateSession(session), 7); err != nil {
		t.Fatal(err)
	}
	if err := g.SendTo(ctx, 7, 1, []byte("direct")); err != nil {
		t.Fatalf("SendTo bound uid: %v", err)
	}
	n, err := g.Broadcast(ctx, 9, []byte("hi"))
	if err != nil || n != 1 {
		t.Fatalf("broadcast delivered %d, %v; want 1, nil", n, err)
	}
}

// Broadcast 并行投递：一条连接队列满（对端不读）不应拖住其它连接。
func TestGateBroadcastSlowConnDoesNotBlockOthers(t *testing.T) {
	testServer := server.New(server.WithQueueSize(1), server.WithLogger(quietLogger()))
	defer func() { _ = testServer.Shutdown(nil) }()
	g := NewGate(nil, testServer, WithGateLogger(quietLogger()))

	// 慢连接：对端不读，写协程阻塞在管道上；持续投递直到队列满。
	slowServer, slowClient := net.Pipe()
	defer slowClient.Close()
	slow, err := testServer.Connect(slowServer)
	if err != nil {
		t.Fatal(err)
	}
	fill := codec.Msg{ID: 1, Body: []byte("fill")}
	for i := 0; i < 200; i++ {
		if serr := slow.TrySend(fill); errors.Is(serr, server.ErrQueueFull) {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if !errors.Is(slow.TrySend(fill), server.ErrQueueFull) {
		t.Fatal("slow conn queue not full; setup broken")
	}

	// 快连接：正常消费。
	fastServer, fastClient := net.Pipe()
	defer fastClient.Close()
	if _, err := testServer.Connect(fastServer); err != nil {
		t.Fatal(err)
	}
	got := make(chan struct{}, 1)
	go func() {
		c := codec.DefaultCodec(64 * 1024)
		for {
			m, rerr := c.ReadMsg(fastClient)
			if rerr != nil {
				return
			}
			if m.ID == 9 {
				got <- struct{}{}
				return
			}
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
	defer cancel()
	start := time.Now()
	doneCh := make(chan struct{})
	var (
		n    int
		bErr error
	)
	go func() {
		n, bErr = g.Broadcast(ctx, 9, []byte("hi"))
		close(doneCh)
	}()

	select {
	case <-got:
		if elapsed := time.Since(start); elapsed > 400*time.Millisecond {
			t.Fatalf("fast conn starved by slow conn: %v", elapsed)
		}
	case <-time.After(600 * time.Millisecond):
		t.Fatal("fast conn did not receive broadcast while slow conn blocked")
	}
	<-doneCh
	if n != 1 || bErr == nil {
		t.Fatalf("broadcast n=%d err=%v; want 1 and slow-conn ctx error", n, bErr)
	}
}

func gateWriteMsg(t *testing.T, w io.Writer, c codec.Codec, m codec.Msg) {
	t.Helper()
	if err := c.WriteMsg(w, m); err != nil {
		t.Fatal(err)
	}
}

func gateDrainUntilClose(c codec.Codec, remote net.Conn) {
	for {
		if _, err := c.ReadMsg(remote); err != nil {
			return
		}
	}
}
