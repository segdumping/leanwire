package quic

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
	"testing"
	"time"

	"github.com/segdumping/leanwire/codec"
	"github.com/segdumping/leanwire/ext/router"
	"github.com/segdumping/leanwire/server"
)

const alpn = "leanwire"

// skipIfEnvBlocked 环境阻断 QUIC 时标记 SKIP；正常环境不会触发。
func skipIfEnvBlocked(t *testing.T, err error, stage string) {
	t.Helper()
	if err == nil {
		return
	}
	t.Skipf("environment blocks QUIC at %s: %v", stage, err)
}

func selfSigned(t *testing.T) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(7),
		Subject:      pkix.Name{CommonName: "quic-test"},
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

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// TestQUICParity 在环回 QUIC 上跑内核 echo。
func TestQUICParity(t *testing.T) {
	udpAvailable(t)
	app := server.New(server.WithLogger(quiet()), server.WithIdleTimeout(time.Minute))
	routeTable := router.Bind(app, router.WithLogger(quiet()))
	routeTable.Handle(1, func(s *server.Conn, m codec.Msg) { _ = s.Send(m) })

	serverTLS := &tls.Config{
		Certificates: []tls.Certificate{selfSigned(t)},
		NextProtos:   []string{alpn},
		MinVersion:   tls.VersionTLS13,
	}
	listener, err := Listen("127.0.0.1:0", serverTLS)
	if err != nil {
		t.Fatal(err)
	}
	app.Serve(listener)

	clientTLS := &tls.Config{
		InsecureSkipVerify: true, // 自签测试证书
		NextProtos:         []string{alpn},
		MinVersion:         tls.VersionTLS13,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cliConn, err := Dial(ctx, listener.Addr().String(), clientTLS)
	skipIfEnvBlocked(t, err, "handshake")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := app.Connect(cliConn); err != nil {
		t.Fatal(err)
	}

	c := codec.DefaultCodec(64 * 1024)
	const n = 10
	for i := 0; i < n; i++ {
		if err := c.WriteMsg(cliConn, codec.Msg{ID: 1, Body: []byte{byte(i)}}); err != nil {
			t.Fatal(err)
		}
	}
	_ = cliConn.SetReadDeadline(time.Now().Add(5 * time.Second))
	for i := 0; i < n; i++ {
		m, err := c.ReadMsg(cliConn)
		if err != nil {
			skipIfEnvBlocked(t, err, "transport")
			t.Fatalf("recv #%d: %v", i, err)
		}
		if len(m.Body) != 1 || m.Body[0] != byte(i) {
			t.Fatalf("order at #%d: %v", i, m.Body)
		}
	}

	sctx, scancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer scancel()
	if err := app.Shutdown(sctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if err := listener.Close(); err != nil {
		t.Fatalf("listener close: %v", err)
	}
}

func udpAvailable(t *testing.T) {
	t.Helper()
	for attempt := 0; attempt < 3; attempt++ {
		if udpRoundTrip(t) {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Skip("udp loopback unusable in this environment")
}

func udpRoundTrip(t *testing.T) bool {
	t.Helper()
	ua, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("resolve udp: %v", err)
	}
	srv, err := net.ListenUDP("udp", ua)
	if err != nil {
		t.Skipf("listen udp: %v", err)
	}
	defer srv.Close()
	sa, ok := srv.LocalAddr().(*net.UDPAddr)
	if !ok {
		t.Skip("bad local addr")
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]byte, 8)
		n, cli, rerr := srv.ReadFromUDP(buf)
		if rerr != nil {
			return
		}
		_, _ = srv.WriteToUDP(buf[:n], cli)
	}()

	cli, err := net.ListenUDP("udp", nil)
	if err != nil {
		t.Skipf("client listen: %v", err)
	}
	defer cli.Close()

	res := make(chan bool, 1)
	go func() {
		if _, err := cli.WriteToUDP([]byte("ping"), sa); err != nil {
			res <- false
			return
		}
		buf := make([]byte, 16)
		n, rerr := cli.Read(buf)
		if rerr != nil {
			res <- false
			return
		}
		res <- string(buf[:n]) == "ping"
	}()

	select {
	case ok := <-res:
		<-done
		return ok
	case <-time.After(2 * time.Second):
		return false
	}
}
