package app

import (
	"context"

	"github.com/segdumping/leanwire/codec"
	"github.com/segdumping/leanwire/server"
)

// SessionID （server.Conn.ID()）
type SessionID = uint64

type Session interface {
	ID() SessionID
	Conn() *server.Conn
	SendBytes(msgID uint32, body []byte) error
	SendWaitBytes(ctx context.Context, msgID uint32, body []byte) error
}

type session struct {
	id   SessionID
	conn *server.Conn
}

func (s *session) ID() SessionID      { return s.id }
func (s *session) Conn() *server.Conn { return s.conn }

func (s *session) SendBytes(msgID uint32, body []byte) error {
	return s.conn.Send(codec.Msg{ID: msgID, Body: body})
}

func (s *session) SendWaitBytes(ctx context.Context, msgID uint32, body []byte) error {
	return s.conn.SendWait(ctx, codec.Msg{ID: msgID, Body: body})
}

// SessionOption 定制会话包装。
type SessionOption func(*sessionConfig)

type sessionConfig struct {
	idOf func(*server.Conn) SessionID
}

// WithSessionID 覆盖会话 ID 的生成方式（例如 conn→uid 映射）。
func WithSessionID(fn func(*server.Conn) SessionID) SessionOption {
	return func(c *sessionConfig) {
		if fn != nil {
			c.idOf = fn
		}
	}
}

func sessionOptions(opts []SessionOption) sessionConfig {
	conf := sessionConfig{idOf: func(c *server.Conn) SessionID { return c.ID() }}
	for _, opt := range opts {
		opt(&conf)
	}
	return conf
}

func (conf sessionConfig) wrap(c *server.Conn) Session {
	return &session{id: conf.idOf(c), conn: c}
}

// NewSession 手工把连接包装成指定 ID 的会话。
func NewSession(conn *server.Conn, id SessionID) Session {
	if conn == nil {
		panic("app: nil conn")
	}
	return &session{id: id, conn: conn}
}
