package server

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"time"

	"github.com/segdumping/leanwire/codec"
)

var connID atomic.Uint64

func nextID() uint64 { return connID.Add(1) }

type Conn struct {
	conf *config
	id   uint64
	conn net.Conn
	out  chan codec.Msg
	bw   *bufio.Writer
	done chan struct{}

	closed atomic.Bool
	reason error
}

func newConn(server *Server, conn net.Conn) *Conn {
	conf := server.config
	id := nextID()
	return &Conn{
		conf: &conf,
		id:   id,
		conn: conn,
		out:  make(chan codec.Msg, conf.queue),
		bw:   bufio.NewWriterSize(conn, bufSize),
		done: make(chan struct{}),
	}
}

func (s *Conn) ID() uint64            { return s.id }
func (s *Conn) RemoteAddr() net.Addr  { return s.conn.RemoteAddr() }
func (s *Conn) Done() <-chan struct{} { return s.done }
func (s *Conn) closeDone()            { close(s.done) }

// Send 阻塞至入队成功、连接关闭或 ctx 到期
func (s *Conn) Send(ctx context.Context, m codec.Msg) (err error) {
	if s.closed.Load() {
		return ErrClosed
	}
	defer func() {
		if recover() != nil {
			err = ErrClosed
		}
	}()

	// 先试一次非阻塞入队
	select {
	case s.out <- m:
		return nil
	default:
	}

	if ctx == nil {
		t := time.NewTimer(s.conf.flush)
		defer t.Stop()
		select {
		case s.out <- m:
			return nil
		case <-t.C:
			return ErrTimeout
		}
	}

	select {
	case s.out <- m:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// TrySend 非阻塞：队列满丢弃
func (s *Conn) TrySend(m codec.Msg) (err error) {
	if s.closed.Load() {
		return ErrClosed
	}
	defer func() {
		if recover() != nil {
			err = ErrClosed
		}
	}()
	select {
	case s.out <- m:
		return nil
	default:
		return ErrQueueFull
	}
}

func (s *Conn) Close() {
	if s.closed.CompareAndSwap(false, true) {
		close(s.out)
		_ = s.conn.SetReadDeadline(time.Now())
	}
}

func (s *Conn) writeLoop(setReason func(error)) {
	defer s.conn.Close()
	defer s.Close()

	var drainBy time.Time // 零值 = 尚未进入排空
	for m := range s.out {
		deadline := time.Now().Add(s.conf.flush)
		if s.closed.Load() {
			if drainBy.IsZero() {
				drainBy = deadline
			}
			deadline = drainBy
			if !deadline.After(time.Now()) {
				return
			}
		}
		if err := s.conn.SetWriteDeadline(deadline); err != nil {
			setReason(err)
			return
		}
		if err := s.writeMsg(s.bw, m); err != nil {
			setReason(err)
			return
		}
		if err := s.bw.Flush(); err != nil {
			setReason(err)
			return
		}
	}
}

func (s *Conn) writeMsg(w io.Writer, m codec.Msg) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("codec WriteMsg panic: %v", r)
		}
	}()
	return s.conf.codec.WriteMsg(w, m)
}

func (s *Conn) readLoop(stopping func() bool, setReason func(error)) {
	defer s.Close()

	s.conf.hooks.fireOpen(s)
	br := bufio.NewReaderSize(s.conn, bufSize)
	for {
		if s.conf.idle > 0 {
			_ = s.conn.SetReadDeadline(time.Now().Add(s.conf.idle))
		}

		if s.closed.Load() || stopping() {
			setReason(ErrClosed)
			return
		}

		m, err := s.readMsg(br)
		if err != nil {
			if s.closed.Load() || stopping() {
				setReason(ErrClosed)
			} else {
				setReason(readReason(err))
			}
			return
		}

		if root := s.conf.rootHandler; root != nil {
			s.runHandler(root, m)
		} else {
			s.conf.log.Warn("message dropped: no root handler",
				"conn", s.id, "msgID", m.ID, "remote", s.conn.RemoteAddr())
		}
	}
}

func (s *Conn) readMsg(br *bufio.Reader) (m codec.Msg, err error) {
	defer func() {
		if r := recover(); r != nil {
			m, err = codec.Msg{}, fmt.Errorf("codec ReadMsg panic: %v", r)
		}
	}()
	return s.conf.codec.ReadMsg(br)
}

func (s *Conn) runHandler(h HandlerFunc, m codec.Msg) {
	defer func() {
		if r := recover(); r != nil {
			s.onError(fmt.Errorf("handler panic: %v", r))
		}
	}()
	h(s, m)
}

func (s *Conn) onError(err error) {
	s.conf.hooks.fireError(s, err)
	s.conf.log.Error("conn error", "conn", s.id, "remote", s.RemoteAddr(), "err", err)
}

func readReason(err error) error {
	if errors.Is(err, codec.ErrTooLarge) {
		return codec.ErrTooLarge
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return ErrIdle
	}
	return err
}
