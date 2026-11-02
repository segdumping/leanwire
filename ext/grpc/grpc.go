package grpc

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

const (
	serviceName = "leanwire.transport.Transport"
	methodPath  = "/leanwire.transport.Transport/Stream"
	codecName   = "leanwire.raw"

	acceptQueue = 16
)

// adoptWait accept 队列满时的阻塞上限，超时回 ResourceExhausted。
var adoptWait = 5 * time.Second

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

// adoptConn 把连接投递进 accept 队列：队列满时阻塞等待有界时长，
// 与 WS/QUIC 对齐（阻塞而非静默断开），超时由调用方回 ResourceExhausted。
func (l *Listener) adoptConn(c net.Conn) bool {
	l.mu.Lock()
	closed := l.closed
	l.mu.Unlock()
	if closed {
		return false
	}
	timer := time.NewTimer(adoptWait)
	defer timer.Stop()
	for {
		select {
		case l.ch <- c:
			return true
		case <-timer.C:
			return false
		case <-l.done:
			return false
		}
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
		return status.Errorf(codes.ResourceExhausted, "grpc transport: accept queue full")
	}
	select {
	case <-c.closed:
	case <-stream.Context().Done():
		_ = c.Close()
	}
	return nil
}

// Dial 拨出并打开一条双向流作为 net.Conn。
// 建立 NewStream 阶段受 ctx 约束（超时/取消即时失败）；
// 流建立后生命周期与拨号 ctx 解耦——调用方用短超时 ctx 拨号时，
// ctx 到期取消不应随断掉已建立的连接。
func Dial(ctx context.Context, target string, opts ...ggrpc.DialOption) (net.Conn, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	cc, err := ggrpc.NewClient(target, opts...)
	if err != nil {
		return nil, err
	}
	streamContext, cancel := context.WithCancel(context.Background())
	type dialResult struct {
		cs  ggrpc.ClientStream
		err error
	}
	done := make(chan dialResult, 1)
	go func() {
		cs, err := cc.NewStream(streamContext, streamDesc, methodPath, ggrpc.CallContentSubtype(codecName))
		done <- dialResult{cs: cs, err: err}
	}()
	select {
	case r := <-done:
		if r.err != nil {
			cancel()
			_ = cc.Close()
			return nil, r.err
		}
		c := newConn(r.cs, cancel, cc)
		c.local = stringAddr{network: "grpc", addr: "local"}
		c.remote = stringAddr{network: "grpc", addr: target}
		return c, nil
	case <-ctx.Done():
		cancel() // NewStream 迟到成功也会随 ctx/streamContext 关断
		_ = cc.Close()
		return nil, ctx.Err()
	}
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
	wdeadline time.Time
	wdGen     chan struct{}

	sendSlot chan struct{} // 写互斥槽位：同一时刻至多一条 SendMsg 在途，超时放弃但不释放

	in chan []byte

	local  net.Addr
	remote net.Addr
}

var _ net.Conn = (*Conn)(nil)

func newConn(stream streamIO, cancel context.CancelFunc, cc *ggrpc.ClientConn) *Conn {
	c := &Conn{
		stream:   stream,
		cancel:   cancel,
		cc:       cc,
		closed:   make(chan struct{}),
		rdGen:    make(chan struct{}),
		wdGen:    make(chan struct{}),
		sendSlot: make(chan struct{}, 1),
		in:       make(chan []byte, 1),
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

// Write 语义对齐 net.Conn：写 deadline 只影响本次等待，到期返回超时错误，
// 不关流、不记连接级错误，之后仍可重试；重试期间旧发送未完则先等其让出槽位。
func (c *Conn) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	buffer := make([]byte, len(p))
	copy(buffer, p)

	c.wmu.Lock()
	dl := c.wdeadline
	c.wmu.Unlock()

	// 写互斥槽位：同一时刻至多一条 SendMsg 在途。已设 deadline 时等槽同样受其
	// 约束：首次发送超时后在途 SendMsg 仍持槽（如被对端流控卡住），重试的
	// Write 必须能到点返回超时，而不是无限阻塞在槽位上。
	var slotTimer *time.Timer
	var slotTimeout <-chan time.Time
	if !dl.IsZero() {
		if d := time.Until(dl); d <= 0 {
			return 0, deadlineError{} // 快路径：deadline 已过期，不发起发送
		} else {
			slotTimer = time.NewTimer(d)
			slotTimeout = slotTimer.C
		}
	}
	stopSlot := func() {
		if slotTimer != nil {
			slotTimer.Stop()
		}
	}
	select {
	case c.sendSlot <- struct{}{}:
		stopSlot()
	case <-slotTimeout:
		return 0, deadlineError{}
	case <-c.closed:
		stopSlot()
		return 0, net.ErrClosed
	}

	type sendResult struct {
		n   int
		err error
	}
	done := make(chan sendResult, 1)
	go func() {
		err := c.stream.SendMsg(&frame{data: buffer})
		<-c.sendSlot // 发送完成（含出错）才让出槽位
		done <- sendResult{n: len(buffer), err: err}
	}()

	for {
		c.wmu.Lock()
		dl := c.wdeadline
		gen := c.wdGen
		c.wmu.Unlock()

		var timer *time.Timer
		var tch <-chan time.Time
		if !dl.IsZero() {
			if d := time.Until(dl); d <= 0 {
				// 槽位由在途 SendMsg 自行释放；Close 会关断流使其尽快结束
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
		case r := <-done:
			stop()
			if r.err != nil {
				if lerr := c.lastError(); lerr != nil {
					return 0, lerr
				}
				select {
				case <-c.closed:
					return 0, net.ErrClosed
				default:
				}
				return 0, r.err
			}
			return r.n, nil
		case <-tch:
			return 0, deadlineError{}
		case <-gen:
			stop()
			continue
		case <-c.closed:
			stop()
			return 0, net.ErrClosed
		}
	}
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
	close(c.wdGen)
	c.wdGen = make(chan struct{})
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
