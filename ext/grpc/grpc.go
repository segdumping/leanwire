package grpc

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/peer"
)

const (
	serviceName = "leanwire.transport.Transport"
	methodPath  = "/leanwire.transport.Transport/Stream"
	codecName   = "leanwire.raw"

	acceptQueue = 16
)

type frame struct{ data []byte }

type rawCodec struct{}

func (rawCodec) Marshal(v any) ([]byte, error) {
	f, ok := v.(*frame)
	if !ok {
		return nil, fmt.Errorf("grpc transport: unexpected type %T", v)
	}
	return f.data, nil
}

func (rawCodec) Unmarshal(data []byte, v any) error {
	f, ok := v.(*frame)
	if !ok {
		return fmt.Errorf("grpc transport: unexpected type %T", v)
	}
	f.data = append([]byte(nil), data...)
	return nil
}

func (rawCodec) Name() string { return codecName }

func init() { encoding.RegisterCodec(rawCodec{}) }

var streamDesc = &ggrpc.StreamDesc{
	StreamName:    "Stream",
	Handler:       handleStream,
	ServerStreams: true,
	ClientStreams: true,
}

type service struct{ l *Listener }

// Register 把流传输服务注册到 gRPC Server。
func Register(grpcServer *ggrpc.Server, l *Listener) {
	grpcServer.RegisterService(&ggrpc.ServiceDesc{
		ServiceName: serviceName,
		HandlerType: (*any)(nil),
		Streams:     []ggrpc.StreamDesc{*streamDesc},
		Metadata:    "leanwire/ext/grpc transport",
	}, &service{l: l})
}

func handleStream(grpcService any, stream ggrpc.ServerStream) error {
	return grpcService.(*service).l.adopt(stream)
}

type Listener struct {
	mu     sync.Mutex
	closed bool
	ch     chan net.Conn
	done   chan struct{}
	addr   net.Addr
}

func NewListener(addr net.Addr) *Listener {
	if addr == nil {
		addr = stringAddr{network: "grpc", addr: "grpc"}
	}
	return &Listener{
		ch:   make(chan net.Conn, acceptQueue),
		done: make(chan struct{}),
		addr: addr,
	}
}

func (l *Listener) Accept() (net.Conn, error) {
	select {
	case c := <-l.ch:
		return c, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}

// Close 停止接收并关闭未交付连接。
func (l *Listener) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	close(l.done)
	for {
		select {
		case c := <-l.ch:
			_ = c.Close()
		default:
			return nil
		}
	}
}

func (l *Listener) Addr() net.Addr { return l.addr }

func (l *Listener) adoptConn(c net.Conn) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return false
	}
	select {
	case l.ch <- c:
		return true
	default:
		return false
	}
}

func (l *Listener) adopt(stream ggrpc.ServerStream) error {
	c := newConn(stream, nil, nil)
	c.local = l.addr
	if p, ok := peer.FromContext(stream.Context()); ok && p.Addr != nil {
		c.remote = p.Addr
	}
	if !l.adoptConn(c) {
		_ = c.Close()
		return nil
	}
	select {
	case <-c.closed:
	case <-stream.Context().Done():
		_ = c.Close()
	}
	return nil
}

// Dial 拨出并打开一条双向流作为 net.Conn。
func Dial(ctx context.Context, target string, opts ...ggrpc.DialOption) (net.Conn, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	cc, err := ggrpc.NewClient(target, opts...)
	if err != nil {
		return nil, err
	}
	streamContext, cancel := context.WithCancel(ctx)
	cs, err := cc.NewStream(streamContext, streamDesc, methodPath, ggrpc.CallContentSubtype(codecName))
	if err != nil {
		cancel()
		_ = cc.Close()
		return nil, err
	}
	c := newConn(cs, cancel, cc)
	c.local = stringAddr{network: "grpc", addr: "local"}
	c.remote = stringAddr{network: "grpc", addr: target}
	return c, nil
}

type streamIO interface {
	SendMsg(m any) error
	RecvMsg(m any) error
}

type deadlineError struct{}

func (deadlineError) Error() string   { return "grpc transport: i/o timeout" }
func (deadlineError) Timeout() bool   { return true }
func (deadlineError) Temporary() bool { return true }

type Conn struct {
	stream streamIO
	cancel context.CancelFunc
	cc     *ggrpc.ClientConn

	closeOnce sync.Once
	closed    chan struct{}

	errMu   sync.Mutex
	lastErr error

	rmu       sync.Mutex
	pending   []byte
	rdeadline time.Time
	rdGen     chan struct{}

	wmu       sync.Mutex
	wsend     sync.Mutex
	wdeadline time.Time

	in chan []byte

	local  net.Addr
	remote net.Addr
}

var _ net.Conn = (*Conn)(nil)

func newConn(stream streamIO, cancel context.CancelFunc, cc *ggrpc.ClientConn) *Conn {
	c := &Conn{
		stream: stream,
		cancel: cancel,
		cc:     cc,
		closed: make(chan struct{}),
		rdGen:  make(chan struct{}),
		in:     make(chan []byte, 1),
	}
	go c.pump()
	return c
}

func (c *Conn) pump() {
	defer close(c.in)
	for {
		var f frame
		if err := c.stream.RecvMsg(&f); err != nil {
			c.setLastErr(err)
			return
		}
		select {
		case c.in <- f.data:
		case <-c.closed:
			return
		}
	}
}

func (c *Conn) setLastErr(err error) {
	c.errMu.Lock()
	if c.lastErr == nil {
		c.lastErr = err
	}
	c.errMu.Unlock()
}

func (c *Conn) lastError() error {
	c.errMu.Lock()
	defer c.errMu.Unlock()
	return c.lastErr
}

func (c *Conn) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for {
		if n, ok := c.tryRead(p); ok {
			return n, nil
		}

		c.rmu.Lock()
		gen := c.rdGen
		dl := c.rdeadline
		c.rmu.Unlock()

		var timer *time.Timer
		var tch <-chan time.Time
		if !dl.IsZero() {
			if d := time.Until(dl); d <= 0 {
				return 0, deadlineError{}
			} else {
				timer = time.NewTimer(d)
				tch = timer.C
			}
		}
		stop := func() {
			if timer != nil {
				timer.Stop()
			}
		}

		select {
		case msg, ok := <-c.in:
			stop()
			if !ok {
				if err := c.lastError(); err != nil {
					return 0, err
				}
				return 0, io.EOF
			}
			n := copy(p, msg)
			if n < len(msg) {
				c.rmu.Lock()
				c.pending = msg[n:]
				c.rmu.Unlock()
			}
			return n, nil
		case <-tch:
			return 0, deadlineError{}
		case <-gen:
			stop()
			continue
		case <-c.closed:
			stop()
			if n, ok := c.tryRead(p); ok {
				return n, nil
			}
			if err := c.lastError(); err != nil {
				return 0, err
			}
			return 0, net.ErrClosed
		}
	}
}

func (c *Conn) tryRead(p []byte) (int, bool) {
	c.rmu.Lock()
	if len(c.pending) > 0 {
		n := copy(p, c.pending)
		c.pending = c.pending[n:]
		c.rmu.Unlock()
		return n, true
	}
	c.rmu.Unlock()

	select {
	case msg, ok := <-c.in:
		if !ok {
			return 0, false
		}
		n := copy(p, msg)
		if n < len(msg) {
			c.rmu.Lock()
			c.pending = msg[n:]
			c.rmu.Unlock()
		}
		return n, true
	default:
		return 0, false
	}
}

func (c *Conn) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	c.wsend.Lock()
	defer c.wsend.Unlock()

	c.wmu.Lock()
	dl := c.wdeadline
	c.wmu.Unlock()

	buffer := make([]byte, len(p))
	copy(buffer, p)

	var timedOut atomic.Bool
	var timer *time.Timer
	if !dl.IsZero() {
		d := time.Until(dl)
		if d <= 0 {
			return 0, deadlineError{}
		}
		timer = time.AfterFunc(d, func() {
			timedOut.Store(true)
			c.setLastErr(deadlineError{})
			_ = c.Close()
		})
	}

	err := c.stream.SendMsg(&frame{data: buffer})
	if timer != nil {
		timer.Stop()
	}
	if timedOut.Load() {
		return 0, deadlineError{}
	}
	if err != nil {
		if lerr := c.lastError(); lerr != nil {
			return 0, lerr
		}
		select {
		case <-c.closed:
			return 0, net.ErrClosed
		default:
		}
		return 0, err
	}
	return len(p), nil
}

// Close 关闭连接，幂等。
func (c *Conn) Close() error {
	c.closeOnce.Do(func() {
		close(c.closed)
		if c.cancel != nil {
			c.cancel()
		}
		if c.cc != nil {
			_ = c.cc.Close()
		}
	})
	return nil
}

func (c *Conn) LocalAddr() net.Addr  { return c.local }
func (c *Conn) RemoteAddr() net.Addr { return c.remote }

func (c *Conn) SetReadDeadline(t time.Time) error {
	c.rmu.Lock()
	c.rdeadline = t
	close(c.rdGen)
	c.rdGen = make(chan struct{})
	c.rmu.Unlock()
	return nil
}

func (c *Conn) SetWriteDeadline(t time.Time) error {
	c.wmu.Lock()
	c.wdeadline = t
	c.wmu.Unlock()
	return nil
}

func (c *Conn) SetDeadline(t time.Time) error {
	if err := c.SetReadDeadline(t); err != nil {
		return err
	}
	return c.SetWriteDeadline(t)
}

type stringAddr struct{ network, addr string }

func (a stringAddr) Network() string { return a.network }
func (a stringAddr) String() string  { return a.addr }
