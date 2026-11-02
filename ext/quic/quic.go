package quic

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
)

const (
	acceptQueue = 4

	// defaultKeepAlive 缺省传输层保活周期，需小于 quic-go 默认 MaxIdleTimeout(30s)，
	// 静默连接在应用层 idle 之外不再被传输层关断。
	defaultKeepAlive = 15 * time.Second

	acceptErrorBackoff = 100 * time.Millisecond
)

// Option 定制 QUIC 参数。
type Option func(*quic.Config)

// WithConfig 替换整套 quic.Config。
// KeepAlivePeriod 设为 0 表示显式禁用保活；MaxIdleTimeout 设为 0 延用 quic-go 默认 30s。
// 传输层 idle 与内核 server.WithIdleTimeout 是叠加的两道闸，.configure 时须一并评估。
func WithConfig(config *quic.Config) Option {
	return func(cfg *quic.Config) {
		if config != nil {
			*cfg = *config
		}
	}
}

func newConfig(opts []Option) *quic.Config {
	cfg := &quic.Config{KeepAlivePeriod: defaultKeepAlive}
	for _, o := range opts {
		if o != nil {
			o(cfg)
		}
	}
	return cfg
}

type Conn struct {
	stream  *quic.Stream
	local   net.Addr
	remote  net.Addr
	closeFn func() error
}

func (c *Conn) Read(p []byte) (int, error)  { return c.stream.Read(p) }
func (c *Conn) Write(p []byte) (int, error) { return c.stream.Write(p) }

// Close 关闭本流。
func (c *Conn) Close() error {
	err := c.stream.Close()
	if c.closeFn != nil {
		if cerr := c.closeFn(); err == nil {
			err = cerr
		}
	}
	return err
}

func (c *Conn) LocalAddr() net.Addr  { return c.local }
func (c *Conn) RemoteAddr() net.Addr { return c.remote }

func (c *Conn) SetReadDeadline(t time.Time) error  { return c.stream.SetReadDeadline(t) }
func (c *Conn) SetWriteDeadline(t time.Time) error { return c.stream.SetWriteDeadline(t) }

func (c *Conn) SetDeadline(t time.Time) error {
	if err := c.stream.SetReadDeadline(t); err != nil {
		return err
	}
	return c.stream.SetWriteDeadline(t)
}

type Listener struct {
	inner  *quic.Listener
	ctx    context.Context
	cancel context.CancelFunc
	ch     chan net.Conn

	mu    sync.Mutex
	conns map[*quic.Conn]struct{}
}

func Listen(addr string, tlsConf *tls.Config, opts ...Option) (net.Listener, error) {
	ua, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	udpConn, err := net.ListenUDP("udp", ua)
	if err != nil {
		return nil, err
	}
	ql, err := quic.Listen(udpConn, tlsConf, newConfig(opts))
	if err != nil {
		_ = udpConn.Close()
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	l := &Listener{
		inner:  ql,
		ctx:    ctx,
		cancel: cancel,
		ch:     make(chan net.Conn, acceptQueue),
		conns:  make(map[*quic.Conn]struct{}),
	}
	go l.acceptConns()
	return l, nil
}

func (l *Listener) acceptConns() {
	for {
		qc, err := l.inner.Accept(l.ctx)
		if err != nil {
			// 监听已关闭（ctx 结束 / ErrServerClosed 会 Unwrap 到 net.ErrClosed）才退出；
			// 其余传输错误退避后继续，避免 Accept 永久悬挂
			if l.ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			time.Sleep(acceptErrorBackoff)
			continue
		}
		l.addConn(qc)
		go l.acceptStreams(qc)
	}
}

func (l *Listener) addConn(qc *quic.Conn) {
	l.mu.Lock()
	l.conns[qc] = struct{}{}
	l.mu.Unlock()
}

func (l *Listener) dropConn(qc *quic.Conn) {
	l.mu.Lock()
	delete(l.conns, qc)
	l.mu.Unlock()
}

func (l *Listener) acceptStreams(qc *quic.Conn) {
	defer func() {
		l.dropConn(qc)
		if l.ctx.Err() != nil {
			_ = qc.CloseWithError(0, "server closed")
		}
	}()
	for {
		stream, err := qc.AcceptStream(l.ctx)
		if err != nil {
			return
		}
		conn := &Conn{stream: stream, local: qc.LocalAddr(), remote: qc.RemoteAddr()}
		select {
		case l.ch <- conn:
		case <-l.ctx.Done():
			_ = stream.Close()
			return
		}
	}
}

func (l *Listener) Accept() (net.Conn, error) {
	select {
	case c := <-l.ch:
		return c, nil
	case <-l.ctx.Done():
		return nil, net.ErrClosed
	}
}

// Close 关闭 Listener。
func (l *Listener) Close() error {
	l.cancel()
	l.mu.Lock()
	conns := l.conns
	l.conns = make(map[*quic.Conn]struct{})
	l.mu.Unlock()
	for qc := range conns {
		_ = qc.CloseWithError(0, "server closed")
	}
	for {
		select {
		case c := <-l.ch:
			_ = c.Close()
		default:
			return l.inner.Close()
		}
	}
}

func (l *Listener) Addr() net.Addr { return l.inner.Addr() }

func Dial(ctx context.Context, addr string, tlsConf *tls.Config, opts ...Option) (net.Conn, error) {
	ua, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	udpConn, err := net.ListenUDP("udp", nil)
	if err != nil {
		return nil, err
	}
	qc, err := quic.Dial(ctx, udpConn, ua, tlsConf, newConfig(opts))
	if err != nil {
		_ = udpConn.Close()
		return nil, err
	}
	stream, err := qc.OpenStream()
	if err != nil {
		_ = qc.CloseWithError(0, "client closed")
		_ = udpConn.Close()
		return nil, err
	}
	return &Conn{
		stream: stream,
		local:  qc.LocalAddr(),
		remote: qc.RemoteAddr(),
		closeFn: func() error {
			err := qc.CloseWithError(0, "client closed")
			_ = udpConn.Close()
			return err
		},
	}, nil
}
