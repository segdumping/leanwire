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
	SendBytes(ctx context.Context, msgID uint32, body []byte) error
	TrySendBytes(msgID uint32, body []byte) error
}

type session struct {
	id   SessionID
	conn *server.Conn
}

func (s *session) ID() SessionID      { return s.id }
func (s *session) Conn() *server.Conn { return s.conn }

func (s *session) SendBytes(ctx context.Context, msgID uint32, body []byte) error {
	if reservedID(msgID) { // 与 App.Handle/Client.Send 一致：拦截框架保留号
		return ErrReservedID
	}
	return s.conn.Send(ctx, codec.Msg{ID: msgID, Body: body})
}

func (s *session) TrySendBytes(msgID uint32, body []byte) error {
	if reservedID(msgID) {
		return ErrReservedID
	}
	return s.conn.TrySend(codec.Msg{ID: msgID, Body: body})
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
