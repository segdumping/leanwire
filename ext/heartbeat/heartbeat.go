package heartbeat

import (
	"sync"
	"time"

	"github.com/segdumping/leanwire/codec"
	"github.com/segdumping/leanwire/server"
)

const DefaultID = codec.IDHeartbeat

const defaultInterval = 10 * time.Second

type Heartbeat struct {
	id    uint32
	body  []byte
	every time.Duration

	appServer *server.Server

	stopOnce sync.Once
	stop     chan struct{}
}

type Option func(*Heartbeat)

// WithID 指定 msgID，默认 0xF001。
func WithID(id uint32) Option { return func(h *Heartbeat) { h.id = id } }

// WithBody 指定心跳 body。
func WithBody(b []byte) Option { return func(h *Heartbeat) { h.body = b } }

func New(appServer *server.Server, every time.Duration, opts ...Option) *Heartbeat {
	if every <= 0 {
		every = defaultInterval
	}
	h := &Heartbeat{
		id:        DefaultID,
		every:     every,
		appServer: appServer,
		stop:      make(chan struct{}),
	}
	for _, o := range opts {
		o(h)
	}
	go h.loop()
	return h
}

// Stop 停止心跳发送。
func (h *Heartbeat) Stop() { h.stopOnce.Do(func() { close(h.stop) }) }

func (h *Heartbeat) loop() {
	m := codec.Msg{ID: h.id, Body: h.body}
	t := time.NewTicker(h.every)
	defer t.Stop()
	for {
		select {
		case <-h.stop:
			return
		case <-t.C:
			h.appServer.Range(func(s *server.Conn) bool {
				_ = s.TrySend(m)
				return true
			})
		}
	}
}
