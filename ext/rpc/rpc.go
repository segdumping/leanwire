package rpc

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segdumping/leanwire/codec"
	"github.com/segdumping/leanwire/ext/router"
	"github.com/segdumping/leanwire/server"
)

const DefaultID uint32 = codec.IDRPC

const frameHeadLen = 8

const respFlag uint32 = 0x8000_0000

const defaultReplyTimeout = 3 * time.Second

const (
	statusOK  byte = 0
	statusErr byte = 1
)

var (
	ErrClosed      = errors.New("rpc: channel closed")
	ErrReplied     = errors.New("rpc: reply already sent")
	ErrNilCallback = errors.New("rpc: nil callback")
	errShortFrame  = errors.New("rpc: frame too short")
	errBadResp     = errors.New("rpc: bad response")
)

// RemoteError 对端返回的业务错误。
type RemoteError struct {
	Code uint32
	Msg  string
}

func (e *RemoteError) Error() string {
	return fmt.Sprintf("remote error %d: %s", e.Code, e.Msg)
}

// Reply 回包函数，同一请求只能成功回包一次。
type Reply func(payload []byte, err error) error

type Handler func(s *server.Conn, id uint32, body []byte, reply Reply)

type Channel struct {
	id           uint32
	log          *slog.Logger
	replyTimeout time.Duration
	dropRequest  func(*server.Conn) bool
	post         func(uint64, func()) bool
	affinity     func(*server.Conn) uint64

	handlers atomic.Pointer[map[uint32]Handler] // COW：装配期写、运行期读

	seq       atomic.Uint32
	pendingMu sync.Mutex
	pending   map[uint32]*pendingCall

	closed atomic.Bool
}

type pendingCall struct {
	conn *server.Conn

	ch       chan result
	callback func([]byte, error)
	done     chan struct{}
}

type result struct {
	payload []byte
	err     error
}

func (call *pendingCall) finish(payload []byte, err error) {
	if call.done != nil {
		close(call.done)
	}
	if call.ch != nil {
		call.ch <- result{payload: payload, err: err}
		return
	}
	call.callback(payload, err)
}

func (call *pendingCall) abort() {
	if call.done != nil {
		close(call.done)
	}
}

type Option func(*Channel)

// WithID 指定通道消息号。
func WithID(id uint32) Option { return func(c *Channel) { c.id = id } }

// WithLogger 设置日志器。
func WithLogger(l *slog.Logger) Option {
	return func(c *Channel) {
		if l != nil {
			c.log = l
		}
	}
}

// WithReplyTimeout 设置回包投递超时。
func WithReplyTimeout(d time.Duration) Option {
	return func(c *Channel) {
		if d > 0 {
			c.replyTimeout = d
		}
	}
}

// WithRequestFilter 设置入站请求过滤器，返回 true 时丢弃。
func WithRequestFilter(fn func(*server.Conn) bool) Option {
	return func(c *Channel) {
		if fn != nil {
			c.dropRequest = fn
		}
	}
}

// WithPost 设置 handler 与异步回调的执行器。
func WithPost(fn func(uint64, func()) bool) Option {
	return func(c *Channel) {
		if fn != nil {
			c.post = fn
		}
	}
}

// WithAffinity 设置连接对应的执行 key。
func WithAffinity(fn func(*server.Conn) uint64) Option {
	return func(c *Channel) {
		if fn != nil {
			c.affinity = fn
		}
	}
}

func New(routeTable *router.Router, opts ...Option) *Channel {
	c := &Channel{
		id:           DefaultID,
		log:          slog.Default(),
		replyTimeout: defaultReplyTimeout,
		pending:      make(map[uint32]*pendingCall),
	}
	hm := make(map[uint32]Handler)
	c.handlers.Store(&hm)
	for _, o := range opts {
		o(c)
	}
	routeTable.Handle(c.id, c.dispatch)
	return c
}

// Handle 注册请求处理；重复注册覆盖并记 Warn。
func (c *Channel) Handle(id uint32, h Handler) {
	if h == nil {
		panic("rpc: nil handler")
	}
	cur := *c.handlers.Load()
	if _, dup := cur[id]; dup {
		c.log.Warn("rpc: handler overwritten", "msgID", id)
	}
	next := make(map[uint32]Handler, len(cur)+1)
	for k, v := range cur {
		next[k] = v
	}
	next[id] = h
	c.handlers.Store(&next)
}

// MustHandle 是 Handle 的严格版，nil 或重复注册直接 panic。
func (c *Channel) MustHandle(id uint32, h Handler) {
	if h == nil {
		panic("rpc: nil handler")
	}
	if _, ok := (*c.handlers.Load())[id]; ok {
		panic("rpc: duplicate handler for msgID " + strconv.FormatUint(uint64(id), 10))
	}
	c.Handle(id, h)
}

func (c *Channel) Registered(id uint32) bool {
	_, ok := (*c.handlers.Load())[id]
	return ok
}

// Call 同步请求。
func (c *Channel) Call(ctx context.Context, s *server.Conn, id uint32, payload []byte) ([]byte, error) {
	if c.closed.Load() {
		return nil, ErrClosed
	}
	if s == nil {
		return nil, server.ErrClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	call := &pendingCall{conn: s, ch: make(chan result, 1)}
	seq := c.addPending(call)
	defer c.removePending(seq, call)

	if err := s.SendWait(ctx, codec.Msg{ID: c.id, Body: packFrame(seq, id, payload)}); err != nil {
		return nil, err
	}
	select {
	case r := <-call.ch:
		return r.payload, r.err
	case <-s.Done(): // 连接关闭：在途调用快速失败（Done 无登记窗口）
		return nil, server.ErrClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// CallAsync 异步请求，响应或错误通过 callback 返回。
func (c *Channel) CallAsync(ctx context.Context, s *server.Conn, id uint32, payload []byte, callback func([]byte, error)) error {
	if callback == nil {
		return ErrNilCallback
	}
	if c.closed.Load() {
		return ErrClosed
	}
	if s == nil {
		return server.ErrClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	call := &pendingCall{conn: s, callback: c.wrapCallback(c.keyOf(s), callback), done: make(chan struct{})}
	seq := c.addPending(call)

	if err := s.SendWait(ctx, codec.Msg{ID: c.id, Body: packFrame(seq, id, payload)}); err != nil {
		if c.removePending(seq, call) {
			call.abort()
		}
		return err
	}
	go c.watchAsync(ctx, s, seq, call)
	return nil
}

func (c *Channel) keyOf(s *server.Conn) uint64 {
	if c.affinity != nil {
		return c.affinity(s)
	}
	if s != nil {
		return s.ID()
	}
	return 0
}

func (c *Channel) wrapCallback(key uint64, callback func([]byte, error)) func([]byte, error) {
	if c.post == nil {
		return callback
	}
	return func(payload []byte, err error) {
		if !c.post(key, func() { callback(payload, err) }) {
			c.log.Debug("rpc: async callback dropped by executor")
		}
	}
}

func (c *Channel) watchAsync(ctx context.Context, s *server.Conn, seq uint32, call *pendingCall) {
	select {
	case <-ctx.Done():
		if c.removePending(seq, call) {
			call.finish(nil, ctx.Err())
		}
	case <-s.Done():
		if c.removePending(seq, call) {
			call.finish(nil, server.ErrClosed)
		}
	case <-call.done:
	}
}

func (c *Channel) addPending(call *pendingCall) uint32 {
	seq := c.nextSeq()
	c.pendingMu.Lock()
	c.pending[seq] = call
	c.pendingMu.Unlock()
	return seq
}

func (c *Channel) removePending(seq uint32, call *pendingCall) bool {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	if c.pending[seq] != call {
		return false
	}
	delete(c.pending, seq)
	return true
}

func (c *Channel) Close() { c.closed.Store(true) }

func (c *Channel) dispatch(s *server.Conn, m codec.Msg) {
	seq, id, payload, err := unpackFrame(m.Body)
	if err != nil {
		c.log.Debug("rpc: bad frame dropped", "err", err)
		return
	}
	if seq&respFlag != 0 { // 响应
		c.deliver(s, seq&^respFlag, payload)
		return
	}
	if c.dropRequest != nil && c.dropRequest(s) {
		c.log.Debug("rpc: request dropped by filter", "msgID", id, "conn", s.ID())
		return
	}
	h := (*c.handlers.Load())[id]
	if h == nil {
		c.log.Warn("rpc: unhandled request, dropped", "msgID", id, "remote", s.RemoteAddr())
		return
	}
	var once atomic.Bool
	reply := func(resp []byte, rerr error) error {
		if !once.CompareAndSwap(false, true) {
			return ErrReplied
		}
		return c.reply(s, seq, id, resp, rerr)
	}
	if c.post != nil {
		if !c.post(c.keyOf(s), func() {
			if c.dropRequest != nil && c.dropRequest(s) {
				c.log.Debug("rpc: request dropped by filter", "msgID", id, "conn", s.ID())
				return
			}
			h(s, id, payload, reply)
		}) {
			c.log.Warn("rpc: handler dropped by executor", "msgID", id, "conn", s.ID())
		}
		return
	}
	h(s, id, payload, reply)
}

func (c *Channel) reply(s *server.Conn, seq, id uint32, resp []byte, rerr error) error {
	status, body := encodeResp(resp, rerr)
	ctx, cancel := context.WithTimeout(context.Background(), c.replyTimeout)
	defer cancel()
	return s.SendWait(ctx, codec.Msg{ID: c.id, Body: packFrame(seq|respFlag, id, append([]byte{status}, body...))})
}

func (c *Channel) deliver(s *server.Conn, seq uint32, payload []byte) {
	c.pendingMu.Lock()
	call := c.pending[seq]
	if call == nil || call.conn != s {
		c.pendingMu.Unlock()
		if call == nil {
			c.log.Debug("rpc: reply for unknown seq", "seq", seq)
		} else {
			c.log.Warn("rpc: reply from wrong conn, dropped", "seq", seq,
				"replyFrom", s.RemoteAddr(), "replyTo", call.conn.RemoteAddr())
		}
		return
	}
	delete(c.pending, seq)
	c.pendingMu.Unlock()

	body, err := decodeResp(payload)
	call.finish(body, err)
}

func (c *Channel) nextSeq() uint32 {
	s := c.seq.Add(1) &^ respFlag
	if s == 0 {
		s = 1
	}
	return s
}

func encodeResp(resp []byte, rerr error) (byte, []byte) {
	if rerr == nil {
		return statusOK, resp
	}
	var re *RemoteError
	if !errors.As(rerr, &re) {
		re = &RemoteError{Code: 0, Msg: rerr.Error()}
	}
	body := make([]byte, 4, 4+len(re.Msg))
	binary.BigEndian.PutUint32(body, re.Code)
	return statusErr, append(body, re.Msg...)
}

func decodeResp(p []byte) ([]byte, error) {
	if len(p) < 1 {
		return nil, errBadResp
	}
	switch p[0] {
	case statusOK:
		return p[1:], nil
	case statusErr:
		if len(p) < 5 {
			return nil, errBadResp
		}
		return nil, &RemoteError{Code: binary.BigEndian.Uint32(p[1:5]), Msg: string(p[5:])}
	}
	return nil, errBadResp
}

func packFrame(seq, id uint32, payload []byte) []byte {
	b := make([]byte, 0, frameHeadLen+len(payload))
	b = binary.BigEndian.AppendUint32(b, seq)
	b = binary.BigEndian.AppendUint32(b, id)
	return append(b, payload...)
}

func unpackFrame(body []byte) (seq, id uint32, payload []byte, err error) {
	if len(body) < frameHeadLen {
		return 0, 0, nil, errShortFrame
	}
	return binary.BigEndian.Uint32(body[:4]),
		binary.BigEndian.Uint32(body[4:8]),
		body[frameHeadLen:],
		nil
}
