package rpc

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
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
	rtA := router.New(router.WithLogger(quietLogger()))
	srvA := server.New(server.WithLogger(quietLogger()), server.WithRootHandler(rtA.Handler()))
	chA := New(rtA, WithLogger(quietLogger()))

	rtB := router.New(router.WithLogger(quietLogger()))
	srvB := server.New(server.WithLogger(quietLogger()), server.WithRootHandler(rtB.Handler()))
	chB := New(rtB, WithLogger(quietLogger()))

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

func TestCallTimeout(t *testing.T) {
	chA, _, connA := newPair(t) // 对端未注册 handler
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	if _, err := chA.Call(ctx, connA, 7, nil); err == nil {
		t.Fatal("want timeout error")
	}
}

func TestCallAsyncTimeout(t *testing.T) {
	chA, _, connA := newPair(t) // 对端未注册 handler
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	if err := chA.CallAsync(ctx, connA, 7, nil, func(_ []byte, err error) {
		done <- err
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("want timeout error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("callback not called on timeout")
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
