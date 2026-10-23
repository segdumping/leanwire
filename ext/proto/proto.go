package proto

import (
	"log/slog"

	gproto "google.golang.org/protobuf/proto"

	"github.com/segdumping/leanwire/codec"
	"github.com/segdumping/leanwire/ext/router"
	"github.com/segdumping/leanwire/server"
)

type PbOf[T any] interface {
	gproto.Message
	*T
}

type Option func(*options)

type options struct{ log *slog.Logger }

// WithLogger 设置日志器。
func WithLogger(l *slog.Logger) Option {
	return func(o *options) {
		if l != nil {
			o.log = l
		}
	}
}

// Register 注册 protobuf 消息处理；解码失败丢弃。
func Register[T any, PT PbOf[T]](routeTable *router.Router, id uint32, h func(s *server.Conn, m *T), opts ...Option) {
	var o options
	o.log = slog.Default()
	for _, f := range opts {
		f(&o)
	}
	routeTable.Handle(id, func(s *server.Conn, msg codec.Msg) {
		m := PT(new(T))
		if err := gproto.Unmarshal(msg.Body, m); err != nil {
			o.log.Debug("proto: unmarshal failed, dropped",
				"msgID", id, "conn", s.ID(), "err", err)
			return
		}
		h(s, m)
	})
}

// Send 序列化并发送 protobuf 消息。
func Send[T any, PT PbOf[T]](s *server.Conn, id uint32, m PT) error {
	body, err := gproto.Marshal(m)
	if err != nil {
		return err
	}
	return s.Send(codec.Msg{ID: id, Body: body})
}
