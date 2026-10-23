package app

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/segdumping/leanwire/codec"
	"github.com/segdumping/leanwire/server"
)

func newSessionTestApp(t *testing.T) *App {
	t.Helper()
	return New("session-test", WithLogger(quietLogger()))
}

func connectSessionTest(t *testing.T, a *App) (*server.Conn, net.Conn) {
	t.Helper()
	serverEnd, testEnd := net.Pipe()
	c, err := a.Server().Connect(serverEnd)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	t.Cleanup(func() {
		_ = a.Server().Shutdown(ctx)
		cancel()
		_ = testEnd.Close()
	})
	return c, testEnd
}

func TestHandleSession(t *testing.T) {
	a := newSessionTestApp(t)
	idCh := make(chan SessionID, 1)
	a.Handle(1100, func(s Session, body []byte) {
		idCh <- s.ID()
		_ = s.SendBytes(1101, body)
	})
	c, testEnd := connectSessionTest(t, a)

	cx := codec.DefaultCodec(64 * 1024)
	if err := cx.WriteMsg(testEnd, codec.Msg{ID: 1100, Body: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	_ = testEnd.SetReadDeadline(time.Now().Add(2 * time.Second))
	m, err := cx.ReadMsg(testEnd)
	if err != nil {
		t.Fatal(err)
	}
	if m.ID != 1101 || string(m.Body) != "x" {
		t.Fatalf("reply = {ID:%d Body:%q}", m.ID, m.Body)
	}
	select {
	case id := <-idCh:
		if id != c.ID() {
			t.Fatalf("session id = %d, want %d", id, c.ID())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not run")
	}
}

func TestHandleWithSessionID(t *testing.T) {
	a := newSessionTestApp(t)
	idCh := make(chan SessionID, 1)
	a.Handle(1104, func(s Session, body []byte) { idCh <- s.ID() },
		WithSessionID(func(*server.Conn) SessionID { return 42 }))

	_, testEnd := connectSessionTest(t, a)
	cx := codec.DefaultCodec(64 * 1024)
	if err := cx.WriteMsg(testEnd, codec.Msg{ID: 1104, Body: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	select {
	case id := <-idCh:
		if id != 42 {
			t.Fatalf("session id = %d, want 42", id)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not run")
	}
}

func TestNewSession(t *testing.T) {
	a := newSessionTestApp(t)
	c, testEnd := connectSessionTest(t, a)

	s := NewSession(c, 7)
	if s.ID() != 7 {
		t.Fatalf("id = %d, want 7", s.ID())
	}
	if s.Conn() != c {
		t.Fatal("Conn() mismatch")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.SendWaitBytes(ctx, 1106, []byte("first")); err != nil {
		t.Fatal(err)
	}
	if err := s.SendWaitBytes(ctx, 1107, []byte("raw")); err != nil {
		t.Fatal(err)
	}

	cx := codec.DefaultCodec(64 * 1024)
	_ = testEnd.SetReadDeadline(time.Now().Add(2 * time.Second))
	for _, want := range []struct {
		id   uint32
		body string
	}{{1106, "first"}, {1107, "raw"}} {
		m, err := cx.ReadMsg(testEnd)
		if err != nil {
			t.Fatal(err)
		}
		if m.ID != want.id || string(m.Body) != want.body {
			t.Fatalf("msg = {ID:%d Body:%q}, want {%d %q}", m.ID, m.Body, want.id, want.body)
		}
	}
}
