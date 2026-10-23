package app

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	"github.com/segdumping/leanwire/codec"
	"github.com/segdumping/leanwire/ext/router"
	"github.com/segdumping/leanwire/server"
)

var (
	ErrInvalidUID   = errors.New("app: gate uid required to be non-zero")
	ErrNoConnection = errors.New("app: gate no connection for uid")
)

type Gate struct {
	app          *App
	clientServer *server.Server
	clientRouter *router.Router

	mu           sync.RWMutex
	byUID        map[uint64]Session
	byConnection map[uint64]uint64

	log *slog.Logger
}

// GateOption 定制 Gate。
type GateOption func(*gateConfig)

type gateConfig struct {
	log          *slog.Logger
	clientRouter *router.Router
}

// WithGateLogger 默认取 App 的日志器（a 为 nil 时用slog.Default()）。
func WithGateLogger(l *slog.Logger) GateOption {
	return func(c *gateConfig) {
		if l != nil {
			c.log = l
		}
	}
}

// WithGateRouter 复用外部已创建的客户端路由表；未设置时 NewGate 自建一个。
func WithGateRouter(clientRouter *router.Router) GateOption {
	return func(c *gateConfig) {
		if clientRouter != nil {
			c.clientRouter = clientRouter
		}
	}
}

func NewGate(a *App, client *server.Server, opts ...GateOption) *Gate {
	if client == nil {
		panic("app: nil gate server")
	}
	if client.Started() {
		panic("app: NewGate called after client Serve/Connect")
	}
	conf := gateConfig{log: slog.Default()}
	if a != nil {
		conf.log = a.Log()
	}
	for _, o := range opts {
		o(&conf)
	}
	if conf.clientRouter == nil {
		conf.clientRouter = router.New(router.WithLogger(conf.log))
	}
	client.SetRootHandler(conf.clientRouter.Handler())

	g := &Gate{
		app:          a,
		clientServer: client,
		clientRouter: conf.clientRouter,
		byUID:        make(map[uint64]Session),
		byConnection: make(map[uint64]uint64),
		log:          conf.log,
	}
	client.AppendHooks(server.Hooks{OnClose: g.doUnbind})
	return g
}

func (g *Gate) App() *App { return g.app }

func (g *Gate) Server() *server.Server { return g.clientServer }

func (g *Gate) Router() *router.Router { return g.clientRouter }

// Bind 把 uid 绑定到连接上的 Session；同uid旧连接被顶
func (g *Gate) Bind(s Session, uid uint64) error {
	if uid == 0 {
		return ErrInvalidUID
	}
	if s == nil || s.Conn() == nil {
		panic("app: nil gate session")
	}
	c := s.Conn()
	bound := NewSession(c, uid)

	var victim Session
	g.mu.Lock()
	if prev, ok := g.byConnection[c.ID()]; ok && prev != uid {
		if cur, ok2 := g.byUID[prev]; ok2 && cur.Conn() == c {
			delete(g.byUID, prev)
		}
	}
	if old, ok := g.byUID[uid]; ok && old.Conn() != c {
		victim = old
	}
	g.byUID[uid] = bound
	g.byConnection[c.ID()] = uid
	g.mu.Unlock()

	if victim != nil {
		victim.Conn().Close() // 锁外关闭，避免持锁走连接收尾
	}
	return nil
}

// Unbind 显式解绑；连接关闭也会自动触发。
func (g *Gate) Unbind(s Session) {
	if s != nil && s.Conn() != nil {
		g.doUnbind(s.Conn(), nil)
	}
}

func (g *Gate) doUnbind(c *server.Conn, _ error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if uid, ok := g.byConnection[c.ID()]; ok {
		delete(g.byConnection, c.ID())
		if cur, ok2 := g.byUID[uid]; ok2 && cur.Conn() == c {
			delete(g.byUID, uid)
		}
	}
}

func (g *Gate) Target(uid uint64) (Session, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	s, ok := g.byUID[uid]
	return s, ok
}

// SendTo 按 uid 投递 payload；队列满时等待至 ctx 到期；无绑定返回 ErrNoConnection。
func (g *Gate) SendTo(ctx context.Context, uid uint64, msgID uint32, payload []byte) error {
	s, ok := g.Target(uid)
	if !ok {
		return ErrNoConnection
	}
	return s.SendWaitBytes(ctx, msgID, payload)
}

const broadcastWorkers = 32

// Broadcast 向全部连接并行广播（至多 broadcastWorkers 路），返回成功投递条数
func (g *Gate) Broadcast(ctx context.Context, msgID uint32, payload []byte) (int, error) {
	var sessions []Session
	g.clientServer.Range(func(c *server.Conn) bool {
		g.mu.RLock()
		uid, ok := g.byConnection[c.ID()]
		var s Session
		if ok {
			if cur, exists := g.byUID[uid]; exists && cur.Conn() == c {
				s = cur
			}
		}
		g.mu.RUnlock()
		if s == nil {
			s = NewSession(c, c.ID())
		}
		sessions = append(sessions, s)
		return true
	})

	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		n    int
		errs []error
	)
	sem := make(chan struct{}, broadcastWorkers)
	for _, s := range sessions {
		wg.Add(1)
		sem <- struct{}{}
		go func(s Session) {
			defer wg.Done()
			defer func() { <-sem }()
			switch err := s.SendWaitBytes(ctx, msgID, payload); {
			case err == nil:
				mu.Lock()
				n++
				mu.Unlock()
			case errors.Is(err, server.ErrClosed):
			default:
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
		}(s)
	}
	wg.Wait()
	return n, errors.Join(errs...)
}

func (g *Gate) sessionOf(c *server.Conn, conf sessionConfig) Session {
	g.mu.RLock()
	if uid, ok := g.byConnection[c.ID()]; ok {
		if s, exists := g.byUID[uid]; exists && s.Conn() == c {
			g.mu.RUnlock()
			return s
		}
	}
	g.mu.RUnlock()
	return conf.wrap(c)
}

// HandleClient 注册客户端消息：未 Bind 的连接 Session.ID 为 conn.ID（或
// WithSessionID 映射值），Bind 后为 uid。消息号显式指定。
func (g *Gate) HandleClient(msgID uint32, h func(s Session, body []byte), opts ...SessionOption) {
	if g.clientRouter == nil {
		panic("app: gate client router required")
	}
	if h == nil {
		panic("app: nil gate client handler")
	}
	conf := sessionOptions(opts)
	g.clientRouter.Handle(msgID, func(c *server.Conn, m codec.Msg) {
		s, body := g.sessionOf(c, conf), m.Body
		g.post(uint64(s.ID()), func() { h(s, body) })
	})
}

func (g *Gate) post(key uint64, fn func()) bool {
	if g.app == nil {
		fn()
		return true
	}
	return g.app.post(key, fn)
}

// HandleServer 注册内部服务消息
func (g *Gate) HandleServer(msgID uint32, h func(s Session, body []byte), opts ...SessionOption) {
	if g.app == nil {
		panic("app: gate has no app")
	}
	g.app.Handle(msgID, h, opts...)
}

func (g *Gate) Handle(msgID uint32, forward func(s Session, uid uint64, payload []byte)) {
	if g.clientRouter == nil {
		panic("app: gate forward router required")
	}
	if forward == nil {
		panic("app: nil gate forward handler")
	}
	g.clientRouter.Handle(msgID, func(c *server.Conn, m codec.Msg) {
		uid, ok := g.uidOf(c)
		if !ok {
			g.log.Debug("app: gate forward from unbound conn dropped",
				"msgID", msgID, "conn", c.ID())
			return
		}
		s, body := NewSession(c, uid), m.Body
		g.post(uid, func() { forward(s, uid, body) })
	})
}

func (g *Gate) uidOf(c *server.Conn) (uint64, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	uid, ok := g.byConnection[c.ID()]
	return uid, ok
}
