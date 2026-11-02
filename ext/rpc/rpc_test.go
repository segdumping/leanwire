package rpc

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/segdumping/leanwire/ext/router"
	"github.com/segdumping/leanwire/server"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func newPair(t *testing.T) (*Channel, *Channel, *server.Conn) {
	t.Helper()
	return newPairOpts(t, nil, nil)
}

func newPairOpts(t *testing.T, optsA, optsB []Option) (*Channel, *Channel, *server.Conn) {
	t.Helper()
	rtA := router.New(router.WithLogger(quietLogger()))
	srvA := server.New(server.WithLogger(quietLogger()), server.WithRootHandler(rtA.Handler()))
	chA := New(rtA, append([]Option{WithLogger(quietLogger())}, optsA...)...)

	rtB := router.New(router.WithLogger(quietLogger()))
	srvB := server.New(server.WithLogger(quietLogger()), server.WithRootHandler(rtB.Handler()))
	chB := New(rtB, append([]Option{WithLogger(quietLogger())}, optsB...)...)

	aEnd, bEnd := net.Pipe()
	connA, err := srvA.Connect(aEnd)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := srvB.Connect(bEnd); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = srvA.Shutdown(ctx)
		_ = srvB.Shutdown(ctx)
	})
	return chA, chB, connA
}

func TestFrameGolden(t *testing.T) {
	got := packFrame(7, 9, []byte("hi"))
	want := []byte{0, 0, 0, 7, 0, 0, 0, 9, 'h', 'i'}
	if !bytes.Equal(got, want) {
		t.Fatalf("got % x, want % x", got, want)
	}
	seq, id, payload, err := unpackFrame(want)
	if err != nil || seq != 7 || id != 9 || string(payload) != "hi" {
		t.Fatalf("unpack: %d %d %q %v", seq, id, payload, err)
	}
	if _, _, _, err := unpackFrame([]byte{1, 2, 3}); err != errShortFrame {
		t.Fatalf("short frame: want errShortFrame, got %v", err)
	}
}

func TestCallSync(t *testing.T) {
	chA, chB, connA := newPair(t)
	chB.Handle(7, func(_ *server.Conn, id uint32, body []byte, reply Reply) {
		if id != 7 {
			t.Errorf("id = %d", id)
		}
		_ = reply(append([]byte("pong:"), body...), nil)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := chA.Call(ctx, connA, 7, []byte("ping"))
	if err != nil {
		t.Fatal(err)
	}
	if string(resp) != "pong:ping" {
		t.Fatalf("resp = %q", resp)
	}
}

func TestCallAsync(t *testing.T) {
	chA, chB, connA := newPair(t)
	chB.Handle(7, func(_ *server.Conn, _ uint32, body []byte, reply Reply) {
		_ = reply(append([]byte("async:"), body...), nil)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan result, 1)
	if err := chA.CallAsync(ctx, connA, 7, []byte("ping"), func(resp []byte, err error) {
		done <- result{payload: resp, err: err}
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-done:
		if r.err != nil || string(r.payload) != "async:ping" {
			t.Fatalf("resp = %q, err = %v", r.payload, r.err)
		}
	case <-ctx.Done():
		t.Fatal("async callback not called")
	}
}

func TestCallNoHandler(t *testing.T) {
	chA, _, connA := newPair(t) // 对端未注册 handler
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := chA.Call(ctx, connA, 7, nil)
	re, ok := err.(*RemoteError)
	if !ok || re.Code != 0 || re.Msg != ErrNoHandler.Error() {
		t.Fatalf("err = %v, want RemoteError{0, %q}", err, ErrNoHandler)
	}
}

func TestCallAsyncNoHandler(t *testing.T) {
	chA, _, connA := newPair(t) // 对端未注册 handler
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	if err := chA.CallAsync(ctx, connA, 7, nil, func(_ []byte, err error) {
		done <- err
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if _, ok := err.(*RemoteError); !ok {
			t.Fatalf("err = %v, want RemoteError", err)
		}
	case <-ctx.Done():
		t.Fatal("callback not called on no handler")
	}
}

func TestCallFiltered(t *testing.T) {
	chA, _, connA := newPairOpts(t, nil, []Option{
		WithRequestFilter(func(*server.Conn) bool { return true }),
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := chA.Call(ctx, connA, 7, nil); !isDropError(err) {
		t.Fatalf("err = %v, want RemoteError{%q}", err, ErrRequestDropped)
	}
}

func TestCallDroppedByExecutor(t *testing.T) {
	chA, chB, connA := newPairOpts(t, nil, []Option{
		WithPost(func(uint64, func()) bool { return false })}, // 执行器拒收：必须回包而非静默
	)
	chB.Handle(7, func(_ *server.Conn, _ uint32, _ []byte, _ Reply) {})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := chA.Call(ctx, connA, 7, nil); !isDropError(err) {
		t.Fatalf("err = %v, want RemoteError{%q}", err, ErrRequestDropped)
	}
}

func TestRemoteError(t *testing.T) {
	chA, chB, connA := newPair(t)
	chB.Handle(7, func(_ *server.Conn, _ uint32, _ []byte, reply Reply) {
		_ = reply(nil, &RemoteError{Code: 42, Msg: "no such uid"})
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := chA.Call(ctx, connA, 7, nil)
	re, ok := err.(*RemoteError)
	if !ok || re.Code != 42 || re.Msg != "no such uid" {
		t.Fatalf("err = %v, want RemoteError{42, no such uid}", err)
	}
}

func TestCallAsyncNilCallback(t *testing.T) {
	ch, _, conn := newPair(t)
	if err := ch.CallAsync(context.Background(), conn, 7, nil, nil); err != ErrNilCallback {
		t.Fatalf("err = %v, want ErrNilCallback", err)
	}
}

// isDropError 拦截类错误经编解码在调用侧表现为 RemoteError，消息即原错误文案。
func isDropError(err error) bool {
	re, ok := err.(*RemoteError)
	return ok && re.Msg == ErrRequestDropped.Error()
}

func TestConcurrentHandle(t *testing.T) {
	rt := router.New(router.WithLogger(quietLogger()))
	ch := New(rt, WithLogger(quietLogger()))
	const n = 200
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		id := uint32(1000 + i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			ch.Handle(id, func(_ *server.Conn, _ uint32, _ []byte, _ Reply) {})
		}()
	}
	wg.Wait()
	for i := 0; i < n; i++ {
		if !ch.Registered(uint32(1000 + i)) {
			t.Fatalf("msgID %d lost by concurrent registration", 1000+i)
		}
	}
}

func TestMustHandleDuplicatePanics(t *testing.T) {
	ch, _, _ := newPair(t)
	ch.MustHandle(7, func(_ *server.Conn, _ uint32, _ []byte, _ Reply) {})
	panicked := false
	func() {
		defer func() { panicked = recover() != nil }()
		ch.MustHandle(7, func(_ *server.Conn, _ uint32, _ []byte, _ Reply) {})
	}()
	if !panicked {
		t.Fatal("want duplicate registration panic")
	}
}

func TestCloseSettlesPendingAsync(t *testing.T) {
	chA, chB, connA := newPair(t)
	chB.Handle(7, func(_ *server.Conn, _ uint32, _ []byte, _ Reply) {}) // 不回包，模拟对端悬挂
	done := make(chan error, 1)
	if err := chA.CallAsync(context.Background(), connA, 7, nil, func(_ []byte, err error) {
		done <- err
	}); err != nil {
		t.Fatal(err)
	}
	chA.Close()
	select {
	case err := <-done:
		if err != ErrClosed {
			t.Fatalf("err = %v, want ErrClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("pending call not settled on close")
	}
}

func TestCloseSettlesPendingCall(t *testing.T) {
	chA, chB, connA := newPair(t)
	chB.Handle(7, func(_ *server.Conn, _ uint32, _ []byte, _ Reply) {}) // 不回包，模拟对端悬挂
	got := make(chan error, 1)
	go func() {
		// ctx 无超时：Close 清算后仍须返回，不能悬挂
		_, err := chA.Call(context.Background(), connA, 7, nil)
		got <- err
	}()
	time.Sleep(50 * time.Millisecond)
	chA.Close()
	select {
	case err := <-got:
		if err != ErrClosed {
			t.Fatalf("err = %v, want ErrClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("pending call not settled on close")
	}
}

// Close 之后 addPending 必须拒绝：不会落入 Close 已遍历过的新 pending 表。
func TestAddPendingAfterClose(t *testing.T) {
	ch := &Channel{}
	ch.Close()

	if _, err := ch.addPending(&pendingCall{}); err != ErrClosed {
		t.Fatalf("addPending after Close = %v, want ErrClosed", err)
	}
	ch.pendingMu.Lock()
	n := len(ch.pending)
	ch.pendingMu.Unlock()
	if n != 0 {
		t.Fatalf("pending table has %d entries after Close", n)
	}
}
