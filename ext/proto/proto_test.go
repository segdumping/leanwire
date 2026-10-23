package proto

import (
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	gproto "google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/segdumping/leanwire/codec"
	"github.com/segdumping/leanwire/ext/router"
	"github.com/segdumping/leanwire/server"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func marshal(m gproto.Message) []byte {
	b, err := gproto.Marshal(m)
	if err != nil {
		panic(err)
	}
	return b
}

// Unmarshal 进 handler、Marshal 出 Send；坏体丢弃不断开连接。
func TestRegisterAndSend(t *testing.T) {
	a := server.New(server.WithLogger(quietLogger()))
	defer func() { _ = a.Shutdown(nil) }()

	got := make(chan *wrapperspb.StringValue, 4)
	routeTable := router.Bind(a)
	Register(routeTable, 50, func(s *server.Conn, m *wrapperspb.StringValue) {
		got <- m
	})
	Register(routeTable, 51, func(s *server.Conn, m *wrapperspb.StringValue) {
		_ = Send(s, 52, wrapperspb.String("reply:"+m.Value))
	})

	serverEnd, testEnd := net.Pipe()
	if _, err := a.Connect(serverEnd); err != nil {
		t.Fatal(err)
	}
	c := codec.DefaultCodec(64 * 1024)

	// 注入合法消息
	if err := c.WriteMsg(testEnd, codec.Msg{ID: 50, Body: marshal(wrapperspb.String("hello"))}); err != nil {
		t.Fatal(err)
	}
	select {
	case m := <-got:
		if m.Value != "hello" {
			t.Fatalf("decoded value %q", m.Value)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("handler not invoked")
	}

	// 坏体：handler 不触发，连接存活
	if err := c.WriteMsg(testEnd, codec.Msg{ID: 50, Body: []byte{0xFF, 0x01}}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-got:
		t.Fatal("bad frame must not be decoded")
	case <-time.After(100 * time.Millisecond):
	}

	// 触发回包方向（Send）
	if err := c.WriteMsg(testEnd, codec.Msg{ID: 51, Body: marshal(wrapperspb.String("x"))}); err != nil {
		t.Fatal(err)
	}
	_ = testEnd.SetReadDeadline(time.Now().Add(1 * time.Second))
	m, err := c.ReadMsg(testEnd)
	if err != nil {
		t.Fatalf("reply read: %v", err)
	}
	if m.ID != 52 {
		t.Fatalf("reply id %d, want 52", m.ID)
	}
	var rep wrapperspb.StringValue
	if err := gproto.Unmarshal(m.Body, &rep); err != nil {
		t.Fatal(err)
	}
	if rep.Value != "reply:x" {
		t.Fatalf("reply value %q", rep.Value)
	}
}
