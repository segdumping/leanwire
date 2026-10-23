// Package server
// 提供应用编排：连接生命周期、有界发送队列、干净停止，
// 内核不持有路由表：收到完整帧即透传给根回调
package server

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segdumping/leanwire/codec"
)

var (
	ErrClosed    = errors.New("conn closed")
	ErrStopping  = errors.New("server stopping")
	ErrQueueFull = errors.New("send queue full")
	ErrIdle      = errors.New("idle timeout")
	ErrTimeout   = errors.New("send msg timeout")
)

const (
	defaultQueueSize = 1024
	defaultIdle      = 10 * time.Minute
	defaultMaxBody   = 64 * 1024
	bufSize          = 4096
	defaultFlush     = 3 * time.Second // 写超时与停止排空上限
)

type HandlerFunc func(s *Conn, m codec.Msg)

type Hooks struct {
	OnOpen  func(*Conn)
	OnClose func(*Conn, error) // 关闭原因：EOF/ErrIdle/ErrClosed/读写错误
	OnError func(*Conn, error) // 非致命错误：handler panic、Accept 错误
}

type config struct {
	codec codec.Codec
	idle  time.Duration
	queue int
	hooks hookList
	log   *slog.Logger
	flush time.Duration

	rootHandler HandlerFunc
	rootSet     bool
}

type Option func(*config)

// WithRootHandler 设置消息回调
func WithRootHandler(fn HandlerFunc) Option {
	return func(conf *config) {
		if conf.rootSet {
			panic("server: root handler already set")
		}
		conf.rootHandler = fn
		conf.rootSet = true
	}
}

// SetRootHandler 设置根回调；连接在创建时快照配置，因此只影响后续连接。
func (server *Server) SetRootHandler(fn HandlerFunc) {
	server.mu.Lock()
	defer server.mu.Unlock()
	if server.config.rootSet {
		panic("server: root handler already set")
	}
	server.config.rootHandler = fn
	server.config.rootSet = true
}

// WithCodec 设置编解码器
func WithCodec(c codec.Codec) Option { return func(conf *config) { conf.codec = c } }

// WithIdleTimeout 设置空闲超时，默认 10 分钟；0 = 关闭。
func WithIdleTimeout(d time.Duration) Option { return func(conf *config) { conf.idle = d } }

// WithQueueSize 设置每连接发送队列容量，默认 1024。
func WithQueueSize(n int) Option { return func(conf *config) { conf.queue = n } }

// WithHooks 注册一组观测钩子（可多次，按注册序执行）。
func WithHooks(h Hooks) Option { return func(conf *config) { conf.hooks = append(conf.hooks, h) } }

// WithLogger 设置日志器，默认 slog.Default()。
func WithLogger(l *slog.Logger) Option { return func(conf *config) { conf.log = l } }

// WithFlushTimeout 设置写超时与停止排空上限，默认 3s。
func WithFlushTimeout(d time.Duration) Option { return func(conf *config) { conf.flush = d } }

type Server struct {
	config

	mu        sync.Mutex
	conns     map[uint64]*Conn
	listeners []net.Listener

	started  bool
	stopping atomic.Bool

	allConnWG sync.WaitGroup // 全部连接协程
}

func New(opts ...Option) *Server {
	conf := config{
		codec: codec.DefaultCodec(defaultMaxBody),
		idle:  defaultIdle,
		queue: defaultQueueSize,
		log:   slog.Default(),
		flush: defaultFlush,
	}
	for _, o := range opts {
		o(&conf)
	}
	if conf.codec == nil {
		conf.codec = codec.DefaultCodec(defaultMaxBody)
	}
	if conf.queue <= 0 {
		conf.queue = defaultQueueSize
	}
	if conf.log == nil {
		conf.log = slog.Default()
	}
	if conf.flush <= 0 {
		conf.flush = defaultFlush
	}
	return &Server{
		config: conf,
		conns:  make(map[uint64]*Conn),
	}
}

func (server *Server) Start() error {
	server.mu.Lock()
	defer server.mu.Unlock()
	if server.stopping.Load() {
		return ErrStopping
	}
	server.started = true
	return nil
}

// Serve 接管 listener 并后台 accept
func (server *Server) Serve(listener net.Listener) error {
	server.mu.Lock()
	if server.stopping.Load() {
		server.mu.Unlock()
		_ = listener.Close()
		return ErrStopping
	}
	server.started = true
	server.listeners = append(server.listeners, listener)
	server.mu.Unlock()

	go server.acceptLoop(listener)
	return nil
}

// Connect 把一条已建立的连接接入内核（出站）；连接由 Server 管理并关闭。
func (server *Server) Connect(conn net.Conn) (*Conn, error) {
	server.mu.Lock()
	if server.stopping.Load() {
		server.mu.Unlock()
		_ = conn.Close()
		return nil, ErrClosed
	}
	server.started = true
	c := newConn(server, conn)
	server.conns[c.id] = c
	server.allConnWG.Add(2)
	server.mu.Unlock()

	server.spawnConn(c)
	return c, nil
}

func (server *Server) spawnConn(c *Conn) {
	var (
		reasonMu sync.Mutex
		reason   error
	)
	setReason := func(err error) {
		reasonMu.Lock()
		if reason == nil {
			reason = err
		}
		reasonMu.Unlock()
	}
	getReason := func() error {
		reasonMu.Lock()
		defer reasonMu.Unlock()
		return reason
	}

	writerDone := make(chan struct{})

	go func() {
		defer server.allConnWG.Done()
		defer close(writerDone)
		c.writeLoop(setReason)
	}()

	go func() {
		defer server.allConnWG.Done()
		defer func() {
			c.Close()
			<-writerDone
			c.closeDone()
			server.removeConn(c.id)
			c.conf.hooks.fireClose(c, getReason())
		}()
		c.readLoop(server.stopping.Load, setReason)
	}()
}

func (server *Server) Get(id uint64) (*Conn, bool) {
	server.mu.Lock()
	defer server.mu.Unlock()
	s, ok := server.conns[id]
	return s, ok
}

func (server *Server) Range(f func(*Conn) bool) {
	server.mu.Lock()
	ss := make([]*Conn, 0, len(server.conns))
	for _, s := range server.conns {
		ss = append(ss, s)
	}
	server.mu.Unlock()
	for _, s := range ss {
		if !f(s) {
			return
		}
	}
}

func (server *Server) Started() bool {
	server.mu.Lock()
	defer server.mu.Unlock()
	return server.started
}

// AppendHooks 追加一组钩子（先注册先执行）；须在 Serve/Connect/Run 前调用，之后 panic。
func (server *Server) AppendHooks(h Hooks) {
	server.mu.Lock()
	defer server.mu.Unlock()
	if server.started {
		panic("server: AppendHooks called after Serve/Connect")
	}
	server.hooks = append(server.hooks, h)
}

type hookList []Hooks

func (hl hookList) fireOpen(s *Conn) {
	for _, h := range hl {
		if h.OnOpen != nil {
			h.OnOpen(s)
		}
	}
}

func (hl hookList) fireClose(s *Conn, err error) {
	for _, h := range hl {
		if h.OnClose != nil {
			h.OnClose(s, err)
		}
	}
}

func (hl hookList) fireError(s *Conn, err error) {
	for _, h := range hl {
		if h.OnError != nil {
			h.OnError(s, err)
		}
	}
}

func (server *Server) Shutdown(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	server.stopping.Store(true)

	server.mu.Lock()
	listeners := server.listeners
	server.listeners = nil
	connections := make([]*Conn, 0, len(server.conns))
	for _, c := range server.conns {
		connections = append(connections, c)
	}
	server.mu.Unlock()

	for _, listener := range listeners {
		_ = listener.Close()
	}

	for _, c := range connections {
		c.Close()
	}

	done := make(chan struct{})
	go func() {
		server.allConnWG.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

var (
	acceptFailLimit = 32
	acceptRetryBase = 10 * time.Millisecond
	acceptRetryMax  = 500 * time.Millisecond // 单次退避封顶
)

func (server *Server) acceptLoop(listener net.Listener) {
	fails := 0
	for {
		conn, err := listener.Accept()
		if err != nil {
			if server.stopping.Load() || errors.Is(err, net.ErrClosed) {
				return
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() { // 瞬时超时不计失败
				fails = 0
				continue
			}
			fails++
			// 失败风暴只在首条与放弃前触发 OnError
			if fails == 1 {
				server.onError(err)
			} else {
				server.log.Error("listener accept failed", "count", fails, "err", err)
			}
			if fails >= acceptFailLimit {
				server.onError(err)
				return
			}
			// 退避封顶 500ms，加抖动防多 listener 同步重试
			sleep := min(time.Duration(fails)*acceptRetryBase, acceptRetryMax)
			sleep = sleep/2 + time.Duration(rand.Int64N(int64(sleep/2)+1))
			time.Sleep(sleep)
			continue
		}
		fails = 0

		server.mu.Lock()
		if server.stopping.Load() {
			server.mu.Unlock()
			_ = conn.Close()
			return
		}
		c := newConn(server, conn)
		server.conns[c.id] = c
		server.allConnWG.Add(2)
		server.mu.Unlock()

		server.spawnConn(c)
	}
}

func (server *Server) removeConn(id uint64) {
	server.mu.Lock()
	delete(server.conns, id)
	server.mu.Unlock()
}

func (server *Server) onError(err error) {
	server.hooks.fireError(nil, err)
	server.log.Warn("listener error", "err", err)
}
