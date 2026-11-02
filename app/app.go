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
//  2. 执行 AppendStarting 钩子；入站派发由 ready 门控，任何 handler 都在 AppendStarting
//     完成之后才执行（提早在读侧阻塞等待，天然背压不缓冲）；
//  3. 起听、向目录注册自身（失败转入后台退避重试，不阻塞 Run）；
//  4. 阻塞至 SIGINT/SIGTERM 或 Exit()；
//  5. 停止：摘除目录 → AppendStopping（连接仍开放，可收尾 Send；钩子限时执行）→
//     停出站连接 → 内核 Shutdown（关连接、限时排空、等待读写协程；Gate
//     持有的 client Server 随内部 Server 同预算并发关闭）→ AppendStopped（倒序）。
//
// 退出期（IsExit 为真后）拒收新的单向消息与请求，只继续处理响应。
//
// 目录与依赖：Depend 声明出站依赖，每个依赖对应一个 name（服务名/division）
// 的单条连接；地址由 ext/registry.Resolver 订阅，断开后由 ext/peer 退避重连。
// 可选 ext/lb.Picker 只在重拨时选 endpoint，不做调用级轮转。按业务键的路由
// （例如 uid 落到固定节点）属业务层：为每个 division/节点单独声明 Depend。
//
// Run 之后仍可调用 Depend / Registry；Handle、Serve、AppendStarting/AppendStopping/AppendStopped
// 必须在 Run 前调用，之后 panic。RPC handler 通过 RPC().Handle 注册，
// 支持运行期更新（注册有锁，同号后注册覆盖前者并记 Warn）。
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
	defaultStopTimeout       = 10 * time.Second
	defaultHeartbeatInterval = 10 * time.Second
	defaultWorkers           = 1
	registerRetryBase        = 500 * time.Millisecond
	registerRetryMax         = 5 * time.Second
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

type appOptions struct {
	name        string
	log         *slog.Logger
	codec       codec.Codec
	idle        time.Duration
	idleSet     bool
	heartbeat   time.Duration
	workers     int
	queueSize   int
	stopTimeout time.Duration
	reg         registry.Registry
	serviceName string
	advertise   string
}

// Option 配置 App。
type Option func(*appOptions)

// WithLogger 默认 slog.Default()。
func WithLogger(l *slog.Logger) Option {
	return func(o *appOptions) {
		if l != nil {
			o.log = l.With("app", o.name)
		}
	}
}

// WithIdleTimeout 显式配置连接空闲超时；未配置时用内核默认（10 分钟）
func WithIdleTimeout(d time.Duration) Option {
	return func(o *appOptions) {
		o.idle = d
		o.idleSet = true
	}
}

// WithRegistry 设置目录：Run 时以目录名（WithServiceName，缺省 App 名）
func WithRegistry(r registry.Registry) Option {
	return func(o *appOptions) {
		o.reg = r
	}
}

// WithServiceName 覆盖目录注册名（默认取 App 名）
func WithServiceName(name string) Option {
	return func(o *appOptions) {
		if name != "" {
			o.serviceName = name
		}
	}
}

func WithCodec(c codec.Codec) Option {
	return func(o *appOptions) {
		o.codec = c
	}
}

// WithAdvertise 覆盖注册用的对外地址；缺省用首个 listener 的实际地址。
func WithAdvertise(addr string) Option {
	return func(o *appOptions) {
		o.advertise = addr
	}
}

// WithHeartbeat 启用连接保活与失效连接检测
func WithHeartbeat(d time.Duration) Option {
	return func(o *appOptions) {
		if d <= 0 {
			d = defaultHeartbeatInterval
		}
		o.heartbeat = d
	}
}

// WithStopTimeout 设置停止总预算：worker 排空、AppendStopping/AppendStopped 限时执行与
// 内核 Shutdown 共享同一预算，默认 10s。
func WithStopTimeout(d time.Duration) Option {
	return func(o *appOptions) {
		if d > 0 {
			o.stopTimeout = d
		}
	}
}

// WithWorkers 设置 worker 数量，默认 1；入站回调靠连接 ID 固定落到同一
// worker（Bind 改写 Session ID 也不会破坏单连接保序）。
func WithWorkers(n int) Option {
	return func(o *appOptions) {
		if n > 0 {
			o.workers = n
		}
	}
}

// WithQueueSize 设置每条连接的发送队列容量，默认 1024。
func WithQueueSize(n int) Option {
	return func(o *appOptions) {
		if n > 0 {
			o.queueSize = n
		}
	}
}

type App struct {
	appOptions
	name string
	log  *slog.Logger

	// 内核与运行期组件
	appServer     *server.Server
	rpcChannel    *rpc.Channel
	messageRouter *router.Router
	worker        *shard.Pool
	hb            *heartbeat.Heartbeat

	// 生命周期状态机
	ctx           context.Context
	cancel        context.CancelFunc
	started       atomic.Bool
	workerClosing atomic.Bool
	stopping      bool
	exitCh        chan struct{}
	exitOnce      sync.Once
	readyCh       chan struct{}
	done          chan struct{}
	stopOnce      sync.Once
	runErr        error

	mu        sync.Mutex
	lifecycle struct {
		start    []func(*App)
		stopping []func(*App)
		stopped  []func(*App)
	}
	listeners     []net.Listener
	clients       map[string]*Client
	clientServers []*server.Server
	dereg         func()
}

func New(name string, opts ...Option) *App {
	o := &appOptions{
		name:        name,
		log:         slog.Default().With("app", name),
		stopTimeout: defaultStopTimeout,
		workers:     defaultWorkers,
	}
	for _, opt := range opts {
		opt(o)
	}
	a := &App{
		name:       name,
		appOptions: *o,
		log:        o.log,
		exitCh:     make(chan struct{}),
		readyCh:    make(chan struct{}),
		done:       make(chan struct{}),
		clients:    make(map[string]*Client),
	}
	a.ctx, a.cancel = context.WithCancel(context.Background())
	a.worker = shard.New(o.workers, shard.WithLogger(a.log))

	serverOptions := []server.Option{server.WithLogger(a.log)}
	if o.codec != nil {
		serverOptions = append(serverOptions, server.WithCodec(o.codec))
	}
	if o.queueSize > 0 {
		serverOptions = append(serverOptions, server.WithQueueSize(o.queueSize))
	}

	switch {
	case o.heartbeat > 0 && !o.idleSet:
		serverOptions = append(serverOptions, server.WithIdleTimeout(3*o.heartbeat))
	case o.idleSet:
		serverOptions = append(serverOptions, server.WithIdleTimeout(o.idle))
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
	if o.heartbeat > 0 {
		a.hb = heartbeat.New(a.appServer, o.heartbeat)
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
	select {
	case <-a.readyCh:
	case <-a.ctx.Done():
		return false
	}
	return a.worker.Post(key, fn)
}

func (a *App) Post(fn func()) bool {
	return a.post(0, fn)
}

func (a *App) TryPost(fn func()) bool {
	if fn == nil || a.worker == nil || a.workerClosing.Load() {
		return false
	}
	select {
	case <-a.readyCh:
	case <-a.ctx.Done():
		return false
	default:
		return false
	}
	return a.worker.TryPost(0, fn)
}

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
		a.post(c.ID(), func() {
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

func (a *App) AppendStarting(f func(*App)) {
	a.beforeRun("AppendStarting")
	if f == nil {
		panic("app: nil hook")
	}
	a.mu.Lock()
	a.lifecycle.start = append(a.lifecycle.start, f)
	a.mu.Unlock()
}

func (a *App) AppendStopping(f func(*App)) {
	a.beforeRun("AppendStopping")
	if f == nil {
		panic("app: nil hook")
	}
	a.mu.Lock()
	a.lifecycle.stopping = append(a.lifecycle.stopping, f)
	a.mu.Unlock()
}

func (a *App) AppendStopped(f func(*App)) {
	a.beforeRun("AppendStopped")
	if f == nil {
		panic("app: nil hook")
	}
	a.mu.Lock()
	a.lifecycle.stopped = append(a.lifecycle.stopped, f)
	a.mu.Unlock()
}

func (a *App) Listen(addr string) error {
	a.beforeRun("Serve")
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
	a.mu.Lock()
	a.beforeRun("Serve")
	a.listeners = append(a.listeners, listener)
	a.mu.Unlock()
}

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
	starts := append([]func(*App){}, a.lifecycle.start...)
	a.mu.Unlock()

	for _, client := range clients {
		client.start()
	}
	for _, f := range starts {
		f(a)
	}
	close(a.readyCh)
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
		a.stopping = true
		dereg := a.dereg
		a.dereg = nil
		clients := make([]*Client, 0, len(a.clients))
		for _, client := range a.clients {
			clients = append(clients, client)
		}
		clientServers := append([]*server.Server(nil), a.clientServers...)
		stops := append([]func(*App){}, a.lifecycle.stopping...)
		finis := append([]func(*App){}, a.lifecycle.stopped...)
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
			a.runHook(ctx, "AppendStopping", f)
		}
		for _, client := range clients {
			client.stop()
		}

		var shutdownWG sync.WaitGroup
		for _, cs := range clientServers {
			shutdownWG.Add(1)
			go func() {
				defer shutdownWG.Done()
				if err := cs.Shutdown(ctx); err != nil {
					a.log.Warn("app: client server shutdown", "err", err)
				}
			}()
		}
		serr := a.appServer.Shutdown(ctx)
		shutdownWG.Wait()

		for _, fini := range slices.Backward(finis) {
			a.runHook(ctx, "AppendStopped", fini)
		}
		a.runErr = serr
		close(a.done)
	})
}

func (a *App) runHook(ctx context.Context, name string, f func(*App)) {
	hookDone := make(chan struct{})
	go func() {
		defer close(hookDone)
		f(a)
	}()
	select {
	case <-hookDone:
	case <-ctx.Done():
		a.log.Warn("app: lifecycle hook exceeded stop budget", "hook", name)
	}
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
		tcp, ok := listeners[0].Addr().(*net.TCPAddr)
		if !ok || tcp.IP == nil || tcp.IP.IsUnspecified() {
			a.log.Info("app: advertise not set and listener addr is unspecified, skip self-register (set WithAdvertise)")
			return
		}
		addr = listeners[0].Addr().String()
	}
	name := a.name
	if a.serviceName != "" {
		name = a.serviceName
	}
	go a.registerLoop(name, addr)
}

func (a *App) registerLoop(name, addr string) {
	backoff := registerRetryBase
	for {
		d, err := a.reg.Register(a.ctx, name, registry.Endpoint{Addr: addr})
		if err == nil {
			a.mu.Lock()
			if a.stopping { // 已停机：立即摘除，迟到的句柄不登记
				a.mu.Unlock()
				d()
				return
			}
			a.dereg = d
			a.mu.Unlock()
			a.log.Info("app: self-registered", "name", name, "addr", addr)
			return
		}
		if a.ctx.Err() != nil {
			return
		}
		a.log.Warn("app: register failed", "addr", addr, "err", err)
		select {
		case <-a.ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, registerRetryMax)
	}
}

func (a *App) beforeRun(what string) {
	if a.started.Load() {
		panic("app: " + what + " called after Run")
	}
}
