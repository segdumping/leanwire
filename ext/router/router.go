package router

import (
	"log/slog"
	"strconv"
	"sync"

	"github.com/segdumping/leanwire/codec"
	"github.com/segdumping/leanwire/server"
)

type table map[uint32]server.HandlerFunc

type unknownFn func(s *server.Conn, m codec.Msg)

type Router struct {
	log    *slog.Logger
	ufn    unknownFn
	server *server.Server

	mu  sync.RWMutex
	tbl table
}

type Option func(*Router)

// WithUnknown 覆盖未知帧处置；缺省丢弃。
func WithUnknown(f func(*server.Conn, codec.Msg)) Option {
	return func(r *Router) {
		if f != nil {
			r.ufn = f
		}
	}
}

// WithLogger 设置日志器。
func WithLogger(l *slog.Logger) Option {
	return func(r *Router) {
		if l != nil {
			r.log = l
		}
	}
}

func New(opts ...Option) *Router {
	r := &Router{log: slog.Default()}
	for _, o := range opts {
		o(r)
	}
	if r.log == nil {
		r.log = slog.Default()
	}
	r.tbl = make(table)
	return r
}

// Bind 创建路由表并接管 Server 的根回调。
func Bind(server *server.Server, opts ...Option) *Router {
	if server == nil {
		panic("router: nil server")
	}
	r := New(opts...)
	r.server = server
	server.SetRootHandler(r.Handler())
	return r
}

// Handler 返回内核根回调。
func (r *Router) Handler() server.HandlerFunc {
	return r.dispatch
}

func (r *Router) dispatch(s *server.Conn, m codec.Msg) {
	r.mu.RLock()
	h := r.tbl[m.ID]
	ufn := r.ufn
	r.mu.RUnlock()
	if h != nil {
		h(s, m)
		return
	}
	if ufn != nil {
		ufn(s, m)
		return
	}
	dropUnknown(r.log, s, m)
}

func dropUnknown(log *slog.Logger, s *server.Conn, m codec.Msg) {
	log.Debug("message dropped: unhandled msgID",
		"conn", s.ID(), "msgID", m.ID, "remote", s.RemoteAddr())
}

// Handle 注册 handler；重复注册覆盖并记 Warn。
func (r *Router) Handle(id uint32, h server.HandlerFunc) {
	if h == nil {
		panic("router: nil handler")
	}
	if r.server != nil && r.server.Started() {
		panic("router: handle after server started")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, dup := r.tbl[id]; dup {
		r.log.Warn("router: handler overwritten", "msgID", id)
	}
	r.tbl[id] = h
}

// MustHandle 是 Handle 的严格版，nil 或重复注册直接 panic。
func (r *Router) MustHandle(id uint32, h server.HandlerFunc) {
	if h == nil {
		panic("router: nil handler")
	}
	if r.server != nil && r.server.Started() {
		panic("router: handle after server started")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.tbl[id]; ok {
		panic("router: duplicate handler for msgID " + strconv.FormatUint(uint64(id), 10))
	}
	r.tbl[id] = h
}

func (r *Router) Registered(id uint32) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.tbl[id]
	return ok
}
