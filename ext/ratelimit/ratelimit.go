package ratelimit

import (
	"log/slog"
	"sync"
	"time"

	"github.com/segdumping/leanwire/codec"
	"github.com/segdumping/leanwire/server"
)

const (
	defaultQPS   = 200
	defaultBurst = 400
)

type Limiter struct {
	log     *slog.Logger
	rate    float64
	burst   float64
	perConn bool
	global  *bucket

	buckets sync.Map
}

type Options struct {
	log     *slog.Logger
	rate    int
	burst   int
	perConn bool
	global  int
}

type Option func(*Options)

// WithLogger 设置日志器。
func WithLogger(l *slog.Logger) Option { return func(o *Options) { o.log = l } }

// WithQPS 设置连接桶速率，<=0 禁用连接限流。
func WithQPS(n int) Option {
	return func(o *Options) {
		if n > 0 {
			o.rate, o.perConn = n, true
		} else {
			o.rate, o.perConn = 0, false
		}
	}
}

// WithBurst 设置连接桶容量。
func WithBurst(n int) Option {
	return func(o *Options) {
		if n > 0 {
			o.burst = n
		}
	}
}

// WithGlobalQPS 启用全局桶。
func WithGlobalQPS(n int) Option {
	return func(o *Options) {
		if n > 0 {
			o.global = n
		}
	}
}

func New(appServer *server.Server, opts ...Option) *Limiter {
	o := Options{rate: defaultQPS, burst: defaultBurst, perConn: true}
	for _, f := range opts {
		f(&o)
	}
	log := o.log
	if log == nil {
		log = slog.Default()
	}
	l := &Limiter{
		log:     log,
		rate:    float64(o.rate),
		burst:   float64(o.burst),
		perConn: o.perConn,
	}
	if o.global > 0 {
		l.global = &bucket{rate: float64(o.global), burst: float64(o.global), tokens: float64(o.global), last: time.Now()}
	}
	appServer.AppendHooks(server.Hooks{OnClose: func(s *server.Conn, _ error) {
		l.buckets.Delete(s.ID())
	}})
	return l
}

// Wrap 给 handler 包限流，桶空即丢。
func (l *Limiter) Wrap(fn server.HandlerFunc) server.HandlerFunc {
	return func(s *server.Conn, m codec.Msg) {
		if reason := l.reason(s); reason != "" {
			l.log.Debug("ratelimit: drop", "reason", reason, "conn", s.ID(), "msgID", m.ID)
			return
		}
		fn(s, m)
	}
}

func (l *Limiter) reason(s *server.Conn) string {
	now := time.Now()
	if l.perConn {
		id := s.ID()
		var b *bucket
		if v, ok := l.buckets.Load(id); ok {
			b = v.(*bucket)
		} else {
			nb := &bucket{rate: l.rate, burst: l.burst, tokens: l.burst, last: now}
			v, _ := l.buckets.LoadOrStore(id, nb)
			b = v.(*bucket)
		}
		if !b.allow(now) {
			return "conn"
		}
	}
	if l.global != nil && !l.global.allow(now) {
		return "global"
	}
	return ""
}

type bucket struct {
	mu     sync.Mutex
	rate   float64
	burst  float64
	tokens float64
	last   time.Time
}

func (b *bucket) allow(now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if e := now.Sub(b.last); e > 0 {
		b.tokens += e.Seconds() * b.rate
		if b.tokens > b.burst {
			b.tokens = b.burst
		}
		b.last = now
	}
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}
