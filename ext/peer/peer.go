package peer

import (
	"errors"
	"log/slog"
	"math/rand/v2"
	"net"
	"sync"
	"time"

	"github.com/segdumping/leanwire/server"
)

const (
	defaultMinDelay = 500 * time.Millisecond
	defaultMaxDelay = 10 * time.Second
)

type Reconn struct {
	appServer *server.Server
	dial      func() (net.Conn, error)

	onConnected    func(*server.Conn)
	onDisconnected func(*server.Conn)
	onFailed       func(err error)

	minDelay time.Duration
	maxDelay time.Duration

	log *slog.Logger

	stopOnce  sync.Once
	startOnce sync.Once
	stopCh    chan struct{}
}

type Option func(*Reconn)

// WithBackoff 设置重拨间隔。
func WithBackoff(min, max time.Duration) Option {
	return func(r *Reconn) {
		if min > 0 {
			r.minDelay = min
		}
		if max >= r.minDelay {
			r.maxDelay = max
		}
	}
}

// WithOnConnected 连接建立回调。
func WithOnConnected(f func(*server.Conn)) Option { return func(r *Reconn) { r.onConnected = f } }

// WithOnDisconnected 连接结束回调。
func WithOnDisconnected(f func(*server.Conn)) Option {
	return func(r *Reconn) { r.onDisconnected = f }
}

// WithOnFailed 拨号或接入失败回调。
func WithOnFailed(f func(err error)) Option { return func(r *Reconn) { r.onFailed = f } }

// WithLogger 设置日志器。
func WithLogger(l *slog.Logger) Option { return func(r *Reconn) { r.log = l } }

func New(appServer *server.Server, dial func() (net.Conn, error), opts ...Option) *Reconn {
	r := &Reconn{
		appServer: appServer,
		dial:      dial,
		minDelay:  defaultMinDelay,
		maxDelay:  defaultMaxDelay,
		log:       slog.Default(),
		stopCh:    make(chan struct{}),
	}
	for _, o := range opts {
		o(r)
	}
	if r.maxDelay < r.minDelay {
		r.maxDelay = r.minDelay
	}
	if r.log == nil {
		r.log = slog.Default()
	}
	return r
}

// Start 启动重连循环，可重复调用。
func (r *Reconn) Start() { r.startOnce.Do(func() { go r.loop() }) }

// Stop 停止后续重拨，不关闭当前连接。
func (r *Reconn) Stop() { r.stopOnce.Do(func() { close(r.stopCh) }) }

func (r *Reconn) loop() {
	delay := r.minDelay
	for {
		select {
		case <-r.stopCh:
			return
		default:
		}

		conn, err := r.dial()
		if err != nil {
			r.fail(err)
			if !r.sleep(delay) {
				return
			}
			delay = r.nextDelay(delay)
			continue
		}
		s, cerr := r.appServer.Connect(conn)
		if cerr != nil {
			_ = conn.Close()
			if errors.Is(cerr, server.ErrClosed) {
				r.log.Info("peer: app shutting down, stop redial")
				return
			}
			r.fail(cerr)
			if !r.sleep(delay) {
				return
			}
			delay = r.nextDelay(delay)
			continue
		}

		early := done(s)
		if r.onConnected != nil {
			r.onConnected(s)
		}
		if early {
			r.fireDisconnected(s)
			if !r.sleep(delay) {
				return
			}
			delay = r.nextDelay(delay)
			continue
		}

		select {
		case <-r.stopCh:
			s.Close()
			r.fireDisconnected(s)
			return
		case <-s.Done():
			r.fireDisconnected(s)
			delay = r.minDelay
		}
	}
}

func done(s *server.Conn) bool {
	select {
	case <-s.Done():
		return true
	default:
		return false
	}
}

func (r *Reconn) fireDisconnected(s *server.Conn) {
	if r.onDisconnected != nil {
		r.onDisconnected(s)
	}
}

func (r *Reconn) fail(err error) {
	r.log.Warn("peer: reconnect failed", "err", err)
	if r.onFailed != nil {
		r.onFailed(err)
	}
}

func (r *Reconn) sleep(d time.Duration) bool {
	select {
	case <-r.stopCh:
		return false
	case <-time.After(d):
		return true
	}
}

func (r *Reconn) nextDelay(d time.Duration) time.Duration {
	d *= 2
	if d > r.maxDelay {
		d = r.maxDelay
	}
	return d/2 + time.Duration(rand.Int64N(int64(d/2)+1))
}
