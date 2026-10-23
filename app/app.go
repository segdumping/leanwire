// Package app
// 提供开箱即用的服务封装：在内核（codec/server）与 ext 之上组合出
// 单进程服务的骨架——消息路由、请求/响应、服务发现与自动重连、有序启停。
// 只封装必要的接口，不含任何业务逻辑。
//
// 执行模型：
//   - Handle 与 RPC 请求回调统一提交到 app worker；同一 key 串行，不同 key
//     可并行。默认 key 是 Session ID，worker 数可用 WithWorkers 配置。
//   - 慢操作会阻塞对应 worker；需要异步时由业务派发。
//   - 不要在回调里同步等待同一连接的 RPC 响应；请求必须传带超时的 ctx。
//
// 投递语义：Send/Reply 经 SendWait 限时入队——队列满时等待，超出 ctx 才返回
// 错误，事件与回包不静默丢弃。心跳帧是唯一例外（允许丢弃）。
//
// 生命周期（Run）：
//  1. 启动各 Depend 的出站连接（拨号循环开始）；
//  2. 执行 OnStart 钩子（在任何入站消息之前）；
//  3. 起听、向目录注册自身；
//  4. 阻塞至 SIGINT/SIGTERM 或 Exit()；
//  5. 停止：摘除目录 → OnStop（连接仍开放，可收尾 Send）→ 停出站连接 →
//     内核 Shutdown（关连接、限时排空、等待读写协程）→ OnFini（倒序）。
//
// 退出期（IsExit 为真后）拒收新的单向消息与请求，只继续处理响应。
//
// 目录与依赖：Depend 声明出站依赖，每个依赖对应一个 name（服务名/division）
// 的单条连接；地址由 ext/registry.Resolver 订阅，断开后由 ext/peer 退避重连。
// 可选 ext/lb.Picker 只在重拨时选 endpoint，不做调用级轮转。按业务键的路由
// （例如 uid 落到固定节点）属业务层：为每个 division/节点单独声明 Depend。
//
// Run 之后仍可调用 Depend / Registry；Handle、Serve、OnStart/OnStop/OnFini
// 必须在 Run 前调用，之后 panic。RPC handler 通过 RPC().Handle 注册，同样
// 须在首个 Serve/Connect 前完成。
package app

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/segdumping/leanwire/codec"
	"github.com/segdumping/leanwire/ext/heartbeat"
	"github.com/segdumping/leanwire/ext/registry"
	"github.com/segdumping/leanwire/ext/router"
	"github.com/segdumping/leanwire/ext/rpc"
	"github.com/segdumping/leanwire/ext/shard"
	"github.com/segdumping/leanwire/server"
)

const (
	// defaultStopTimeout 是停止总预算（OnStop 与内核 Shutdown 共享）。
	defaultStopTimeout = 10 * time.Second
	// defaultHeartbeatInterval 是 WithHeartbeat 未给间隔时的发送周期。
	defaultHeartbeatInterval = 10 * time.Second
	defaultWorkers           = 1
)

var (
	// ErrStarted Run 只能进入一次，二次调用返回此错误。
	ErrStarted = errors.New("app: already running")
	// ErrNotConnected 出站连接未建立（或刚断开）。
	ErrNotConnected = errors.New("app: service not connected")
	// ErrNoDirectory Client 未配置目录与静态地址。
	ErrNoDirectory = errors.New("app: no registry or static address")
	// ErrReservedID 框架保留号不可用于业务消息。
	ErrReservedID = errors.New("app: reserved msg id")
	// ErrStopped 应用已退出，不再接受新的出站依赖。
	ErrStopped = errors.New("app: stopped")
)

func reservedID(id uint32) bool {
	return codec.IsReserved(id)
}

// Option 配置 App。
type Option func(*App)

// WithLogger 默认 slog.Default()。
func WithLogger(l *slog.Logger) Option {
	return func(a *App) {
		if l != nil {
			a.log = l.With("app", a.name)
		}
	}
}

// WithIdleTimeout 显式配置连接空闲超时；未配置时用内核默认（10 分钟）
func WithIdleTimeout(d time.Duration) Option {
	return func(a *App) {
		a.idle = d
		a.idleSet = true
	}
}

// WithRegistry 设置目录：Run 时以目录名（WithServiceName，缺省 App 名）
func WithRegistry(r registry.Registry) Option { return func(a *App) { a.reg = r } }

// WithServiceName 覆盖目录注册名（默认取 App 名）
func WithServiceName(name string) Option {
	return func(a *App) {
		if name != "" {
			a.serviceName = name
		}
	}
}

func WithCodec(c codec.Codec) Option { return func(a *App) { a.codec = c } }

// WithAdvertise 覆盖注册用的对外地址；缺省用首个 listener 的实际地址。
func WithAdvertise(addr string) Option { return func(a *App) { a.advertise = addr } }

// WithHeartbeat 启用连接保活与失效连接检测
func WithHeartbeat(d time.Duration) Option {
	return func(a *App) {
		if d <= 0 {
			d = defaultHeartbeatInterval
		}
		a.heartbeat = d
	}
}

// WithStopTimeout 默认 10s。
func WithStopTimeout(d time.Duration) Option {
	return func(a *App) {
		if d > 0 {
			a.stopTimeout = d
		}
	}
}

// WithWorkers 设置 worker 数量，默认 1；回调按 key 固定落到同一 worker。
func WithWorkers(n int) Option {
	return func(a *App) {
		if n > 0 {
			a.workers = n
		}
	}
}

// WithQueueSize 设置每条连接的发送队列容量，默认 1024。
func WithQueueSize(n int) Option {
	return func(a *App) {
		if n > 0 {
			a.queueSize = n
		}
	}
}

type App struct {
	name string
	log  *slog.Logger

	appServer *server.Server
	codec     codec.Codec

	idle      time.Duration
	idleSet   bool
	heartbeat time.Duration
	hb        *heartbeat.Heartbeat

	rpcChannel    *rpc.Channel
	messageRouter *router.Router
	worker        *shard.Pool
	workers       int
	queueSize     int

	reg         registry.Registry
	serviceName string // 目录注册名（空 = 用 App 名）
	advertise   string
	dereg       func() // 注册句柄；stop 时摘除

	stopTimeout time.Duration

	ctx    context.Context
	cancel context.CancelFunc

	started       atomic.Bool
	workerClosing atomic.Bool
	exitCh        chan struct{}
	exitOnce      sync.Once
	done          chan struct{}
	stopOnce      sync.Once
	runErr        error

	mu        sync.Mutex
	listeners []net.Listener
	clients   map[string]*Client
	onStarts  []func(*App)
	onStops   []func(*App)
	onFinis   []func(*App)
}

func New(name string, opts ...Option) *App {
	a := &App{
		name:        name,
		log:         slog.Default().With("app", name),
		stopTimeout: defaultStopTimeout,
		exitCh:      make(chan struct{}),
		done:        make(chan struct{}),
		clients:     make(map[string]*Client),
		workers:     defaultWorkers,
	}
	for _, o := range opts {
		o(a)
	}
	a.ctx, a.cancel = context.WithCancel(context.Background())
	a.worker = shard.New(a.workers, shard.WithLogger(a.log))

	serverOptions := []server.Option{server.WithLogger(a.log)}
	if a.codec != nil {
		serverOptions = append(serverOptions, server.WithCodec(a.codec))
	}
	if a.queueSize > 0 {
		serverOptions = append(serverOptions, server.WithQueueSize(a.queueSize))
	}

	switch {
	case a.heartbeat > 0 && !a.idleSet:
		serverOptions = append(serverOptions, server.WithIdleTimeout(3*a.heartbeat))
	case a.idleSet:
		serverOptions = append(serverOptions, server.WithIdleTimeout(a.idle))
	}

	a.messageRouter = router.New(router.WithLogger(a.log))
	a.messageRouter.Handle(heartbeat.DefaultID, func(*server.Conn, codec.Msg) {})
	a.rpcChannel = rpc.New(a.messageRouter,
		rpc.WithLogger(a.log),
		rpc.WithRequestFilter(func(*server.Conn) bool { return a.IsExit() }),
		rpc.WithPost(a.post),
		rpc.WithAffinity(func(c *server.Conn) uint64 { return c.ID() }),
	)
	serverOptions = append(serverOptions, server.WithRootHandler(a.messageRouter.Handler()))
	a.appServer = server.New(serverOptions...)
	if a.heartbeat > 0 {
		a.hb = heartbeat.New(a.appServer, a.heartbeat)
	}
	return a
}

func (a *App) Name() string                { return a.name }
func (a *App) Log() *slog.Logger           { return a.log }
func (a *App) Registry() registry.Registry { return a.reg }
func (a *App) Server() *server.Server      { return a.appServer }
func (a *App) RPC() *rpc.Channel           { return a.rpcChannel }

func (a *App) IsExit() bool {
	select {
	case <-a.exitCh:
		return true
	default:
		return false
	}
}

func (a *App) Done() <-chan struct{} { return a.done }

func (a *App) post(key uint64, fn func()) bool {
	if fn == nil || a.worker == nil || a.workerClosing.Load() {
		return false
	}
	return a.worker.Post(key, fn)
}

// Post 把任务投递到 App 的 worker 队列；返回 false 表示 App 已停止。
func (a *App) Post(fn func()) bool {
	return a.post(0, fn)
}

// TryPost 非阻塞投递；队列满或 App 已停止返回 false。
func (a *App) TryPost(fn func()) bool {
	if fn == nil || a.worker == nil || a.workerClosing.Load() {
		return false
	}
	return a.worker.TryPost(0, fn)
}

// AfterPost 延时投递；返回的 cancel 可取消尚未执行的任务。
func (a *App) AfterPost(d time.Duration, fn func()) (cancel func() bool) {
	if fn == nil {
		return func() bool { return false }
	}
	var once sync.Once
	timer := time.AfterFunc(d, func() {
		once.Do(func() {
			a.Post(fn)
		})
	})
	return func() bool {
		cancelled := false
		once.Do(func() {
			cancelled = timer.Stop()
		})
		return cancelled
	}
}

// Handle 注册内部的单向消息处理
func (a *App) Handle(id uint32, h func(s Session, body []byte), opts ...SessionOption) {
	a.beforeRun("Handle")
	if h == nil {
		panic("app: nil handler")
	}
	if reservedID(id) {
		panic(ErrReservedID.Error())
	}
	conf := sessionOptions(opts)
	a.messageRouter.Handle(id, func(c *server.Conn, m codec.Msg) {
		s, body := conf.wrap(c), m.Body
		a.post(uint64(s.ID()), func() {
			if a.IsExit() {
				return
			}
			h(s, body)
		})
	})
}

func (a *App) MustHandle(id uint32, h func(s Session, body []byte), opts ...SessionOption) {
	a.beforeRun("Handle")
	if a.messageRouter.Registered(id) {
		panic("app: duplicate handler for msgID " + strconv.FormatUint(uint64(id), 10))
	}
	a.Handle(id, h, opts...)
}

func (a *App) OnStart(f func(*App)) {
	a.beforeRun("OnStart")
	if f == nil {
		panic("app: nil hook")
	}
	a.mu.Lock()
	a.onStarts = append(a.onStarts, f)
	a.mu.Unlock()
}

func (a *App) OnStop(f func(*App)) {
	a.beforeRun("OnStop")
	if f == nil {
		panic("app: nil hook")
	}
	a.mu.Lock()
	a.onStops = append(a.onStops, f)
	a.mu.Unlock()
}

func (a *App) OnFini(f func(*App)) {
	a.beforeRun("OnFini")
	if f == nil {
		panic("app: nil hook")
	}
	a.mu.Lock()
	a.onFinis = append(a.onFinis, f)
	a.mu.Unlock()
}

func (a *App) Listen(addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	a.Serve(listener)
	return nil
}

func (a *App) Serve(listener net.Listener) {
	if listener == nil {
		panic("app: nil listener")
	}
	a.beforeRun("Serve")
	a.mu.Lock()
	a.listeners = append(a.listeners, listener)
	a.mu.Unlock()
}

// Depend 声明对某个 name（服务名或 division）的依赖
func (a *App) Depend(name string, opts ...ClientOption) *Client {
	a.mu.Lock()
	defer a.mu.Unlock()
	if client, ok := a.clients[name]; ok {
		a.log.Warn("app: service redeclared, reuse", "service", name)
		return client
	}
	if a.IsExit() {
		return &Client{app: a, name: name, broken: ErrStopped}
	}
	client := newClient(a, name, opts)
	a.clients[name] = client
	if a.started.Load() {
		client.start()
	}
	return client
}

// Run 阻塞至停止完成：SIGINT/SIGTERM 或 Exit() 触发。
func (a *App) Run() error {
	if !a.started.CompareAndSwap(false, true) {
		return ErrStarted
	}

	a.mu.Lock()
	listeners := append([]net.Listener(nil), a.listeners...)
	clients := make([]*Client, 0, len(a.clients))
	for _, client := range a.clients {
		clients = append(clients, client)
	}
	starts := append([]func(*App){}, a.onStarts...)
	a.mu.Unlock()

	for _, client := range clients {
		client.start()
	}
	for _, f := range starts {
		f(a)
	}
	if err := a.appServer.Start(); err != nil {
		a.log.Error("app: server run failed", "err", err)
		a.Exit()
	}
	for _, listener := range listeners {
		if err := a.appServer.Serve(listener); err != nil {
			a.log.Error("app: serve failed", "err", err)
			a.Exit()
			break
		}
	}

	a.registerSelf()
	a.log.Info("app: running")

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	select {
	case sig := <-sigCh:
		a.log.Info("app: exiting on signal", "signal", sig.String())
		a.Exit()
	case <-a.exitCh:
		a.log.Info("app: exiting")
	}

	a.stop()
	<-a.done
	return a.runErr
}

func (a *App) Exit() { a.exitOnce.Do(func() { close(a.exitCh) }) }

func (a *App) stop() {
	a.stopOnce.Do(func() {
		a.Exit()

		a.mu.Lock()
		dereg := a.dereg
		clients := make([]*Client, 0, len(a.clients))
		for _, client := range a.clients {
			clients = append(clients, client)
		}
		stops := append([]func(*App){}, a.onStops...)
		finis := append([]func(*App){}, a.onFinis...)
		a.mu.Unlock()

		if dereg != nil {
			dereg()
		}
		a.cancel()
		if a.hb != nil {
			a.hb.Stop()
		}

		ctx, cancel := context.WithTimeout(context.Background(), a.stopTimeout)
		defer cancel()

		a.workerClosing.Store(true)
		if a.worker != nil {
			_ = a.worker.Drain(ctx)
		}
		for _, f := range stops {
			f(a)
		}
		for _, client := range clients {
			client.stop()
		}
		serr := a.appServer.Shutdown(ctx)

		for _, fini := range slices.Backward(finis) {
			fini(a)
		}
		a.runErr = serr
		close(a.done)
	})
}

func (a *App) registerSelf() {
	if a.reg == nil {
		return
	}
	addr := a.advertise
	if addr == "" {
		a.mu.Lock()
		listeners := append([]net.Listener(nil), a.listeners...)
		a.mu.Unlock()
		if len(listeners) == 0 {
			a.log.Info("app: no internal listener, skip self-register")
			return
		}
		addr = routableAddr(listeners[0].Addr())
	}
	name := a.name
	if a.serviceName != "" {
		name = a.serviceName
	}
	d, err := a.reg.Register(a.ctx, name, registry.Endpoint{Addr: addr})
	if err != nil {
		a.log.Warn("app: register failed", "addr", addr, "err", err)
		return
	}
	a.mu.Lock()
	a.dereg = d
	a.mu.Unlock()
}

func routableAddr(addr net.Addr) string {
	host, port := "", ""
	if tcp, ok := addr.(*net.TCPAddr); ok {
		if tcp.IP != nil && !tcp.IP.IsUnspecified() {
			host = tcp.IP.String()
		}
		port = strconv.Itoa(tcp.Port)
	} else {
		h, p, err := net.SplitHostPort(addr.String())
		if err != nil {
			return addr.String()
		}
		host, port = h, p
	}
	if host == "" {
		if port == "" {
			return addr.String()
		}
		return net.JoinHostPort(localRoutableIP(), port)
	}
	if host == "localhost" {
		return net.JoinHostPort(host, port)
	}
	if ip := net.ParseIP(host); ip != nil {
		if ip.IsUnspecified() {
			return net.JoinHostPort(localRoutableIP(), port)
		}
		return net.JoinHostPort(host, port)
	}
	return net.JoinHostPort(host, port)
}

func localRoutableIP() string {
	if conn, err := net.Dial("udp", "8.8.8.8:53"); err == nil {
		defer conn.Close()
		if addr, ok := conn.LocalAddr().(*net.UDPAddr); ok &&
			addr.IP != nil && !addr.IP.IsUnspecified() && !addr.IP.IsLoopback() {
			return addr.IP.String()
		}
	}
	ifaces, err := net.Interfaces()
	if err != nil {
		return "127.0.0.1"
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 || iface.Flags&net.FlagPointToPoint != 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		var fallback string
		for _, a := range addrs {
			ipnet, ok := a.(*net.IPNet)
			if !ok || ipnet.IP.To4() == nil || ipnet.IP.IsLinkLocalUnicast() {
				continue
			}
			if ipnet.IP.IsPrivate() {
				return ipnet.IP.String()
			}
			if fallback == "" {
				fallback = ipnet.IP.String()
			}
		}
		if fallback != "" {
			return fallback
		}
	}
	return "127.0.0.1"
}

func (a *App) beforeRun(what string) {
	if a.started.Load() {
		panic("app: " + what + " called after Run")
	}
}
