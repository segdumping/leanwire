// Package app
// Client 是指向某个 name（服务名或 division）的一条持久连接。

package app

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/segdumping/leanwire/codec"
	"github.com/segdumping/leanwire/ext/lb"
	"github.com/segdumping/leanwire/ext/peer"
	"github.com/segdumping/leanwire/ext/registry"
	"github.com/segdumping/leanwire/server"
)

// defaultDialTimeout 是单次拨号超时（握手）。
const defaultDialTimeout = 3 * time.Second

type Client struct {
	app    *App
	name   string
	broken error

	reconn  *peer.Reconn
	started bool

	mu        sync.Mutex
	conn      *server.Conn
	firstUp   sync.Once
	firstUpCh chan struct{} // WaitReady 的就绪信号（首次连上即关闭）
}

type ClientOption func(*clientConfig)

type clientConfig struct {
	addrs    []string
	picker   lb.Picker
	dial     lb.DialFunc
	minDelay time.Duration
	maxDelay time.Duration
}

// WithAddress 声明静态目标（可多个）；设置后不依赖目录。
func WithAddress(addrs ...string) ClientOption {
	return func(config *clientConfig) { config.addrs = append(config.addrs, addrs...) }
}

// WithPicker 指定重拨时的 endpoint 选择策略（ext/lb.Picker）
func WithPicker(p lb.Picker) ClientOption {
	return func(config *clientConfig) {
		if p != nil {
			config.picker = p
		}
	}
}

// WithDialFunc 覆盖出站拨号（默认 TCP）
func WithDialFunc(f lb.DialFunc) ClientOption {
	return func(config *clientConfig) { config.dial = f }
}

// WithBackoff 覆盖重连退避区间（默认 500ms–10s）。
func WithBackoff(min, max time.Duration) ClientOption {
	return func(config *clientConfig) { config.minDelay, config.maxDelay = min, max }
}

func newClient(application *App, name string, opts []ClientOption) *Client {
	var config clientConfig
	for _, o := range opts {
		o(&config)
	}
	client := &Client{app: application, name: name, firstUpCh: make(chan struct{})}

	reg := application.reg
	if len(config.addrs) > 0 {
		endpoints := make([]registry.Endpoint, 0, len(config.addrs))
		for _, addr := range config.addrs {
			endpoints = append(endpoints, registry.Endpoint{Addr: addr})
		}
		reg = registry.NewStatic(map[string][]registry.Endpoint{name: endpoints})
	}
	if reg == nil {
		client.broken = ErrNoDirectory
		application.log.Warn("app: service has no registry or static address", "service", name)
		return client
	}

	resolver := registry.NewResolver(application.ctx, reg, name)
	dial := func() (net.Conn, error) {
		endpoints := resolver.Endpoints()
		if len(endpoints) == 0 {
			if err := resolver.Err(); err != nil {
				return nil, err
			}
			return nil, registry.ErrNoEndpoint
		}
		endpoint := endpoints[0]
		if config.picker != nil {
			var err error
			endpoint, err = config.picker.Pick(endpoints)
			if err != nil {
				return nil, err
			}
		}
		return dialEndpoint(application, config.dial, endpoint.Addr)
	}
	return newClientWithDial(application, name, dial, config)
}

func newClientWithDial(application *App, name string, dial func() (net.Conn, error), config clientConfig) *Client {
	client := &Client{app: application, name: name, firstUpCh: make(chan struct{})}
	client.reconn = peer.New(application.appServer, dial,
		peer.WithOnConnected(client.attach),
		peer.WithOnDisconnected(client.detach),
		peer.WithBackoff(config.minDelay, config.maxDelay),
		peer.WithLogger(application.log),
	)
	return client
}

func (client *Client) start() {
	client.mu.Lock()
	if client.started || client.reconn == nil {
		client.mu.Unlock()
		return
	}
	client.started = true
	client.mu.Unlock()
	client.reconn.Start()
}

func (client *Client) stop() {
	if client.reconn != nil {
		client.reconn.Stop()
	}
}

func (client *Client) Name() string { return client.name }

func (client *Client) Available() bool {
	client.mu.Lock()
	defer client.mu.Unlock()
	return client.conn != nil
}

func (client *Client) WaitReady(ctx context.Context) error {
	if client.broken != nil {
		return client.broken
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-client.firstUpCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (client *Client) Send(ctx context.Context, id uint32, payload []byte) error {
	if client.broken != nil {
		return client.broken
	}
	if reservedID(id) {
		return ErrReservedID
	}
	s, err := client.current()
	if err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return s.SendWait(ctx, codec.Msg{ID: id, Body: payload})
}

// Call 同步请求：回包与请求共用业务消息号 id
func (client *Client) Call(ctx context.Context, id uint32, payload []byte) ([]byte, error) {
	if client.broken != nil {
		return nil, client.broken
	}
	if reservedID(id) {
		return nil, ErrReservedID
	}
	s, err := client.current()
	if err != nil {
		return nil, err
	}
	return client.app.rpcChannel.Call(ctx, s, id, payload)
}

// CallAsync 异步请求：发送成功后立即返回，响应或错误在 callback 中处理
func (client *Client) CallAsync(ctx context.Context, id uint32, payload []byte, callback func([]byte, error)) error {
	if client.broken != nil {
		return client.broken
	}
	if reservedID(id) {
		return ErrReservedID
	}
	s, err := client.current()
	if err != nil {
		return err
	}
	return client.app.rpcChannel.CallAsync(ctx, s, id, payload, callback)
}

func (client *Client) current() (*server.Conn, error) {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.conn == nil {
		return nil, ErrNotConnected
	}
	return client.conn, nil
}

func (client *Client) attach(s *server.Conn) {
	client.mu.Lock()
	client.conn = s
	client.mu.Unlock()
	client.firstUp.Do(func() { close(client.firstUpCh) })
}

func (client *Client) detach(s *server.Conn) {
	client.mu.Lock()
	if client.conn == s {
		client.conn = nil
	}
	client.mu.Unlock()
}

func dialEndpoint(a *App, f lb.DialFunc, addr string) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(a.ctx, defaultDialTimeout)
	defer cancel()
	if f != nil {
		return f(ctx, addr, defaultDialTimeout)
	}
	var d net.Dialer
	return d.DialContext(ctx, "tcp", addr)
}
