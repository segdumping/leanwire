package ws

import (
	"errors"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type Conn struct {
	ws *websocket.Conn

	mu      sync.Mutex
	wmu     sync.Mutex
	pending []byte
}

const DefaultReadLimit = 64*1024 + 8

const (
	acceptQueue          = 4
	defaultHeaderTimeout = 10 * time.Second
)

func Adapt(c *websocket.Conn) net.Conn { return AdaptWithLimit(c, DefaultReadLimit) }

func AdaptWithLimit(c *websocket.Conn, maxMessageBytes int64) net.Conn {
	if maxMessageBytes > 0 {
		c.SetReadLimit(maxMessageBytes)
	}
	return &Conn{ws: c}
}

func (c *Conn) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for len(c.pending) == 0 {
		t, data, err := c.ws.ReadMessage()
		if err != nil {
			return 0, err
		}
		if t == websocket.BinaryMessage || t == websocket.TextMessage {
			c.pending = data
		}
	}
	n := copy(p, c.pending)
	c.pending = c.pending[n:]
	return n, nil
}

// Write 每条调用写一条 binary 消息。
func (c *Conn) Write(p []byte) (int, error) {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if err := c.ws.WriteMessage(websocket.BinaryMessage, p); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (c *Conn) Close() error                       { return c.ws.Close() }
func (c *Conn) LocalAddr() net.Addr                { return c.ws.LocalAddr() }
func (c *Conn) RemoteAddr() net.Addr               { return c.ws.RemoteAddr() }
func (c *Conn) SetReadDeadline(t time.Time) error  { return c.ws.SetReadDeadline(t) }
func (c *Conn) SetWriteDeadline(t time.Time) error { return c.ws.SetWriteDeadline(t) }

func (c *Conn) SetDeadline(t time.Time) error {
	if err := c.ws.SetReadDeadline(t); err != nil {
		return err
	}
	return c.ws.SetWriteDeadline(t)
}

type listener struct {
	tcp        net.Listener
	httpServer *http.Server
	up         websocket.Upgrader

	doneOnce sync.Once
	done     chan struct{}
	ch       chan net.Conn
}

type Option func(*upgraderCfg)

type upgraderCfg struct {
	checkOrigin func(*http.Request) bool
}

// WithCheckOrigin 覆盖 Origin 校验。
func WithCheckOrigin(f func(*http.Request) bool) Option {
	return func(c *upgraderCfg) { c.checkOrigin = f }
}

func Listen(addr string, opts ...Option) (net.Listener, error) {
	tcpLn, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	var conf upgraderCfg
	for _, o := range opts {
		o(&conf)
	}
	l := &listener{
		tcp:  tcpLn,
		done: make(chan struct{}),
		ch:   make(chan net.Conn, acceptQueue),
	}
	l.up = websocket.Upgrader{CheckOrigin: conf.checkOrigin}
	l.httpServer = &http.Server{
		Handler:           l.handler(),
		ReadHeaderTimeout: defaultHeaderTimeout,
	}
	go func() {
		if err := l.httpServer.Serve(tcpLn); err != nil && !errors.Is(err, http.ErrServerClosed) {
			l.closeDone()
		}
	}()
	return l, nil
}

func (l *listener) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wsConn, err := l.up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		select {
		case l.ch <- Adapt(wsConn):
		case <-l.done:
			_ = wsConn.Close()
		}
	})
}

func (l *listener) Accept() (net.Conn, error) {
	select {
	case c := <-l.ch:
		return c, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}

func (l *listener) Close() error {
	l.closeDone()
	err := l.tcp.Close()
	_ = l.httpServer.Close()
	return err
}

func (l *listener) Addr() net.Addr { return l.tcp.Addr() }

func (l *listener) closeDone() {
	l.doneOnce.Do(func() { close(l.done) })
}
