package app

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

// attach/detach：重连换线后，旧连接迟到的断开不误清新连接。
func TestClientDetachStaleConn(t *testing.T) {
	a := New("x", WithLogger(quietLogger()))
	defer func() { _ = a.Server().Shutdown(nil) }()
	client := &Client{app: a, name: "u1", firstUpCh: make(chan struct{})}

	s1, _ := net.Pipe()
	sess1, err := a.Server().Connect(s1)
	if err != nil {
		t.Fatal(err)
	}
	s2, _ := net.Pipe()
	sess2, err := a.Server().Connect(s2)
	if err != nil {
		t.Fatal(err)
	}

	client.attach(sess1)
	client.attach(sess2)
	client.detach(sess1)
	got, err := client.current()
	if err != nil || got != sess2 {
		t.Fatalf("current = %v, %v; want sess2", got, err)
	}
}

// 应用已退出后声明的依赖是 broken 状态：调用一律报 ErrStopped。
func TestClientBrokenAfterExit(t *testing.T) {
	a := New("x", WithLogger(quietLogger()))
	runApp(a)
	for !a.started.Load() {
		time.Sleep(time.Millisecond)
	}
	a.Exit()
	waitDone(t, a)

	service := a.Depend("late")
	if err := service.WaitReady(context.Background()); !errors.Is(err, ErrStopped) {
		t.Fatalf("WaitReady = %v, want ErrStopped", err)
	}
	if err := service.Send(context.Background(), 1, nil); !errors.Is(err, ErrStopped) {
		t.Fatalf("Send = %v, want ErrStopped", err)
	}
	if _, err := service.Call(context.Background(), 1, nil); !errors.Is(err, ErrStopped) {
		t.Fatalf("Call = %v, want ErrStopped", err)
	}
}

// WaitReady：目标不可达时按 ctx 返回超时，不悬挂。
func TestWaitReadyDeadline(t *testing.T) {
	a := New("x", WithLogger(quietLogger()))
	service := a.Depend("down", WithAddress("127.0.0.1:1"), WithBackoff(20*time.Millisecond, 100*time.Millisecond))
	runApp(a)

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	if err := service.WaitReady(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitReady = %v, want context.DeadlineExceeded", err)
	}
	a.Exit()
	waitDone(t, a)
}

// Send 传 nil ctx：等价 Send 的有界入队语义，消息到达对端。
func TestClientSendNilCtx(t *testing.T) {
	serverApp, addr := newServerApp(t, "service")
	got := make(chan struct{}, 1)
	serverApp.Handle(3, func(s Session, body []byte) { got <- struct{}{} })
	runApp(serverApp)

	clientApp := New("client", WithLogger(quietLogger()))
	service := clientApp.Depend("service", WithAddress(addr), WithBackoff(20*time.Millisecond, 100*time.Millisecond))
	runApp(clientApp)

	ctx := ctxTimeout(t, 2*time.Second)
	if err := service.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}
	if err := service.Send(nil, 3, []byte("shot")); err != nil {
		t.Fatalf("Send(nil ctx): %v", err)
	}
	waitFor(t, 2*time.Second, func() bool {
		select {
		case <-got:
			return true
		default:
			return false
		}
	})
	clientApp.Exit()
	waitDone(t, clientApp)
	serverApp.Exit()
	waitDone(t, serverApp)
}
