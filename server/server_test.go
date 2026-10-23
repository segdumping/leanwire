package server

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"io"
	"log/slog"
	"math/big"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/segdumping/leanwire/codec"
)

// shortFlush 缩短写超时与排空时限，避免测试拖满 3s。
func shortFlush() Option { return WithFlushTimeout(150 * time.Millisecond) }

func newCodec() codec.Codec { return codec.DefaultCodec(64 * 1024) }

// pipePair 返回两端：内核端与测试对端。
func pipePair() (serverEnd, testEnd net.Conn) {
	c1, c2 := net.Pipe()
	return c1, c2
}

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func attachConn(t *testing.T, server *Server, serverEnd net.Conn) *Conn {
	t.Helper()
	s, err := server.Connect(serverEnd)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func writeMsg(t *testing.T, w io.Writer, c codec.Codec, m codec.Msg) {
	t.Helper()
	if err := c.WriteMsg(w, m); err != nil {
		t.Fatal(err)
	}
}

// readAll 从对端持续读帧，直到连接关闭。
func readAll(remote net.Conn, c codec.Codec, msgCh chan<- codec.Msg, errCh chan<- error) {
	for {
		m, err := c.ReadMsg(remote)
		if err != nil {
			errCh <- err
			return
		}
		msgCh <- m
	}
}

type testTable struct {
	mu       sync.RWMutex
	handlers map[uint32]HandlerFunc
}

func newTestTable(server *Server) *testTable {
	table := &testTable{handlers: make(map[uint32]HandlerFunc)}
	server.SetRootHandler(func(c *Conn, m codec.Msg) {
		table.mu.RLock()
		h := table.handlers[m.ID]
		table.mu.RUnlock()
		if h != nil {
			h(c, m)
		}
	})
	return table
}

func (table *testTable) Handle(id uint32, h HandlerFunc) {
	table.mu.Lock()
	table.handlers[id] = h
	table.mu.Unlock()
}

// 5.5 停止顺序：在途 handler 完成，已入队消息不丢

func TestShutdownOrder(t *testing.T) {
	block := make(chan struct{})
	var handlerDone time.Time
	var hookSendErr error
	var onCloseReason error
	onCloseOnce := sync.Once{}

	server := New(WithIdleTimeout(time.Minute), WithHooks(Hooks{
		OnClose: func(s *Conn, err error) {
			onCloseOnce.Do(func() { onCloseReason = err })
		},
	}))
	tt := newTestTable(server)
	tt.Handle(1, func(s *Conn, m codec.Msg) {
		// 停止前已入队的消息必须在关连接前写出
		hookSendErr = s.Send(codec.Msg{ID: 2, Body: []byte("bye")})
		<-block // 模拟在途 handler
		handlerDone = time.Now()
	})

	serverEnd, testEnd := pipePair()
	_ = attachConn(t, server, serverEnd)

	msgCh := make(chan codec.Msg, 8)
	errCh := make(chan error, 1)
	go readAll(testEnd, newCodec(), msgCh, errCh)

	writeMsg(t, testEnd, newCodec(), codec.Msg{ID: 1, Body: []byte("hello")})
	time.Sleep(100 * time.Millisecond) // 等读协程进入阻塞的 handler

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	shutdownErr := make(chan error, 1)
	go func() { shutdownErr <- server.Shutdown(ctx) }()

	time.Sleep(100 * time.Millisecond) // 停止应卡在步骤 3
	close(block)                       // handler 完成，停止继续

	select {
	case err := <-shutdownErr:
		if err != nil {
			t.Fatalf("shutdown: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown did not finish")
	}
	if hookSendErr != nil {
		t.Fatalf("handler Send failed: %v", hookSendErr)
	}
	if handlerDone.IsZero() {
		t.Fatal("handler never completed")
	}
	if onCloseReason != ErrClosed {
		t.Fatalf("OnClose reason = %v, want ErrClosed", onCloseReason)
	}
	// 必须阻塞等待：非阻塞断言会在写协程 flush 完前误报丢消息（历史 flake）。
	select {
	case m := <-msgCh:
		if m.ID != 2 {
			t.Fatalf("got msg id %d, want 2 (queued message)", m.ID)
		}
	case err := <-errCh:
		select {
		case m := <-msgCh:
			if m.ID != 2 {
				t.Fatalf("got msg id %d, want 2 (queued message)", m.ID)
			}
		default:
			t.Fatalf("queued message lost (read err before msg #2: %v)", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("queued message lost (timeout)")
	}
	if len(server.conns) != 0 {
		t.Fatalf("conns not cleaned: %d", len(server.conns))
	}
}

func selfSignedCertificate(t *testing.T) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test.local"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

// TestTLSParity 同一内核跑在 crypto/tls listener 上：echo、顺序与关闭原因。
func TestTLSParity(t *testing.T) {
	serverTLS := &tls.Config{
		Certificates: []tls.Certificate{selfSignedCertificate(t)},
		MinVersion:   tls.VersionTLS12,
	}

	openCh := make(chan *Conn, 1)
	reasonCh := make(chan error, 1)
	server := New(WithIdleTimeout(time.Minute), WithLogger(quietLogger()), WithHooks(Hooks{
		OnOpen:  func(s *Conn) { openCh <- s },
		OnClose: func(s *Conn, err error) { reasonCh <- err },
	}))
	tt := newTestTable(server)
	tt.Handle(1, func(s *Conn, m codec.Msg) { _ = s.Send(m) })

	tcpLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server.Serve(tls.NewListener(tcpLn, serverTLS))

	raw, err := net.Dial("tcp", tcpLn.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	client := tls.Client(raw, &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS12})
	if err := client.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}

	var serverConn *Conn
	select {
	case serverConn = <-openCh:
		if serverConn.ID() == 0 {
			t.Fatal("conn id not assigned")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OnOpen not called")
	}

	c := newCodec()
	const n = 50
	for i := 0; i < n; i++ {
		body := []byte{byte(i >> 24), byte(i >> 16), byte(i >> 8), byte(i)}
		writeMsg(t, client, c, codec.Msg{ID: 1, Body: body})
	}
	for i := 0; i < n; i++ {
		m, err := c.ReadMsg(client)
		if err != nil {
			t.Fatalf("read #%d: %v", i, err)
		}
		got := uint32(m.Body[0])<<24 | uint32(m.Body[1])<<16 | uint32(m.Body[2])<<8 | uint32(m.Body[3])
		if got != uint32(i) {
			t.Fatalf("out of order at #%d: got %d", i, got)
		}
	}

	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-reasonCh:
		if err == nil {
			t.Fatal("close reason must be non-nil")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OnClose not called")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if _, ok := server.Get(serverConn.ID()); ok {
		t.Fatal("conn not removed")
	}
}

// TestTLSPipeParity crypto/tls 会话可经 Connect 进内核。
func TestTLSPipeParity(t *testing.T) {
	cert := selfSignedCertificate(t)
	serverTLS := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}

	server := New(WithIdleTimeout(time.Minute), WithLogger(quietLogger()))
	tt := newTestTable(server)
	tt.Handle(7, func(s *Conn, m codec.Msg) { _ = s.Send(m) })

	srvRaw, cliRaw := net.Pipe()
	srvTLS := tls.Server(srvRaw, serverTLS)
	cliTLS := tls.Client(cliRaw, &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS12})
	_ = cliTLS.SetDeadline(time.Now().Add(10 * time.Second))

	attachConn(t, server, srvTLS)

	c := newCodec()
	writeMsg(t, cliTLS, c, codec.Msg{ID: 7, Body: []byte("ping")})
	m, err := c.ReadMsg(cliTLS)
	if err != nil {
		t.Fatal(err)
	}
	if string(m.Body) != "ping" {
		t.Fatalf("echo mismatch: %q", m.Body)
	}

	// 持续消费服务端输出：net.Pipe 无缓冲，不读会跑满排空预算。
	clientErr := make(chan error, 1)
	go func() {
		for {
			if _, err := c.ReadMsg(cliTLS); err != nil {
				clientErr <- err
				return
			}
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	select {
	case <-clientErr:
	case <-time.After(2 * time.Second):
		t.Fatal("client did not observe connection close")
	}
}
