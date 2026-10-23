package ws

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/segdumping/leanwire/codec"
	"github.com/segdumping/leanwire/ext/router"
	"github.com/segdumping/leanwire/server"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// 内核在 WS 传输上的同套测试：echo、顺序与关闭收尾。
func TestWSParity(t *testing.T) {
	a := server.New(server.WithLogger(quietLogger()), server.WithIdleTimeout(time.Minute))
	routeTable := router.Bind(a)
	routeTable.Handle(1, func(s *server.Conn, m codec.Msg) { _ = s.Send(m) })

	listener, err := Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	a.Serve(listener)

	dialer := &websocket.Dialer{HandshakeTimeout: 3 * time.Second}
	wsConn, _, err := dialer.Dial("ws://"+listener.Addr().String()+"/echo", nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = wsConn.SetReadDeadline(time.Now().Add(5 * time.Second))
	_ = wsConn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	client := Adapt(wsConn) // 客户端也走适配器，保证两端口径一致

	c := codec.DefaultCodec(64 * 1024)
	const n = 20
	for i := 0; i < n; i++ {
		if err := c.WriteMsg(client, codec.Msg{ID: 1, Body: []byte{byte(i)}}); err != nil {
			t.Fatalf("send #%d: %v", i, err)
		}
	}
	for i := 0; i < n; i++ {
		m, err := c.ReadMsg(client)
		if err != nil {
			t.Fatalf("recv #%d: %v", i, err)
		}
		if len(m.Body) != 1 || m.Body[0] != byte(i) {
			t.Fatalf("out of order at #%d: got %v", i, m.Body)
		}
	}

	// WS 关闭后内核连接收尾
	if err := wsConn.WriteControl(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
		time.Now().Add(time.Second)); err != nil {
		t.Fatalf("close control: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := a.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
}

// 并发 Write 必须串行：gorilla 禁止并发 writer，旧实现在此处 panic/交织。
func TestWSConcurrentWrites(t *testing.T) {
	msgCh := make(chan codec.Msg, 64)
	a := server.New(server.WithLogger(quietLogger()), server.WithIdleTimeout(time.Minute))
	routeTable := router.Bind(a)
	routeTable.Handle(1, func(_ *server.Conn, m codec.Msg) {
		select {
		case msgCh <- m:
		default:
		}
	})

	listener, err := Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	a.Serve(listener)

	dialer := &websocket.Dialer{HandshakeTimeout: 3 * time.Second}
	wsConn, _, err := dialer.Dial("ws://"+listener.Addr().String()+"/echo", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer wsConn.Close()
	client := Adapt(wsConn)

	c := codec.DefaultCodec(64 * 1024)
	const n = 32
	var wg sync.WaitGroup
	errCh := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// 整帧一次 Write（并发 WriteMsg 会拆两次 Write，非传输层契约）。
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

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := a.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
}

// 非 WS 请求直接 400，不占住 accept 环。
func TestHTTPRejection(t *testing.T) {
	listener, err := Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Get("http://" + listener.Addr().String() + "/plain")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status %d, want 400", resp.StatusCode)
	}
}
