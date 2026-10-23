package server

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/segdumping/leanwire/codec"
)

// 5.2 同连接顺序
func TestOrdering(t *testing.T) {
	const n = 100
	server := New(WithQueueSize(4096), WithLogger(quietLogger()))
	tt := newTestTable(server)
	tt.Handle(1, func(s *Conn, m codec.Msg) {
		_ = s.Send(m) // echo
	})
	serverEnd, testEnd := pipePair()
	s := attachConn(t, server, serverEnd)

	msgCh := make(chan codec.Msg, n)
	errCh := make(chan error, 1)
	go readAll(testEnd, newCodec(), msgCh, errCh)

	for i := 0; i < n; i++ {
		body := []byte{byte(i >> 24), byte(i >> 16), byte(i >> 8), byte(i)}
		if err := s.Send(codec.Msg{ID: 1, Body: body}); err != nil {
			t.Fatalf("send #%d: %v", i, err)
		}
	}
	for i := 0; i < n; i++ {
		m := <-msgCh
		got := uint32(m.Body[0])<<24 | uint32(m.Body[1])<<16 | uint32(m.Body[2])<<8 | uint32(m.Body[3])
		if got != uint32(i) {
			t.Fatalf("out of order at #%d: got %d", i, got)
		}
	}
	select {
	case <-errCh:
		t.Fatal("connection closed unexpectedly")
	default:
	}
	s.Close()
}

// 5.3 满即丢
func TestQueueFullDrop(t *testing.T) {
	reasonCh := make(chan error, 1)
	server := New(
		shortFlush(),
		WithQueueSize(1),
		WithHooks(Hooks{OnClose: func(s *Conn, err error) { reasonCh <- err }}),
		WithLogger(quietLogger()),
	)
	serverEnd, testEnd := pipePair()
	s := attachConn(t, server, serverEnd)
	defer s.Close()

	// 对端不读：同步写阻塞，队列很快填满（TrySend 非阻塞语义）
	gotFull := false
	for i := 0; i < 64; i++ {
		err := s.TrySend(codec.Msg{ID: 1, Body: []byte("x")})
		if err == ErrQueueFull {
			gotFull = true
			break
		}
		if err != nil {
			t.Fatalf("send #%d: %v (want eventually ErrQueueFull)", i, err)
		}
	}
	if !gotFull {
		t.Fatal("never got ErrQueueFull")
	}

	// 满即丢：连接保持打开
	time.Sleep(100 * time.Millisecond)
	select {
	case err := <-reasonCh:
		t.Fatalf("OnClose fired with %v, want none (Conn must stay open)", err)
	default:
	}

	// 对端恢复读取后 Send 恢复成功
	msgCh := make(chan codec.Msg, 256)
	errCh := make(chan error, 1)
	go readAll(testEnd, newCodec(), msgCh, errCh)

	ok := false
	for i := 0; i < 2000 && !ok; i++ {
		err := s.TrySend(codec.Msg{ID: 1, Body: []byte("y")})
		switch {
		case err == nil:
			ok = true
		case err == ErrQueueFull:
			time.Sleep(2 * time.Millisecond)
		default:
			t.Fatalf("send after drain: %v", err)
		}
	}
	if !ok {
		t.Fatal("Send never recovered after peer started reading")
	}
}

// 5.4 关闭语义：幂等、之后 Send 报错、已入队必达、OnClose 恰一次
func TestCloseFlushesExactlyOnce(t *testing.T) {
	server := New(WithQueueSize(64), WithLogger(quietLogger()))
	closes := 0
	reasonCh := make(chan error, 1)
	closeOnce := sync.Once{}
	server.config.hooks = hookList{{
		OnClose: func(s *Conn, err error) {
			closeOnce.Do(func() {
				closes++
				reasonCh <- err
			})
		},
	}}
	serverEnd, testEnd := pipePair()
	s := attachConn(t, server, serverEnd)

	const n = 10
	for i := 0; i < n; i++ {
		if err := s.Send(codec.Msg{ID: 1, Body: []byte("m")}); err != nil {
			t.Fatalf("send: %v", err)
		}
	}
	s.Close()
	s.Close() // 幂等

	msgCh := make(chan codec.Msg, n+1)
	errCh := make(chan error, 1)
	go readAll(testEnd, newCodec(), msgCh, errCh)

	got := 0
drain:
	for got < n {
		select {
		case <-msgCh:
			got++
		case err := <-errCh:
			// 先排空 msgCh：select 随机仲裁可能先选中 errCh 误报丢帧
		drainMsg:
			for {
				select {
				case <-msgCh:
					got++
				default:
					break drainMsg
				}
			}
			if got != n {
				t.Fatalf("flushed %d/%d before close: %v", got, n, err)
			}
			break drain
		case <-time.After(2 * time.Second):
			t.Fatalf("flush timeout at %d/%d", got, n)
		}
	}
	if err := s.Send(codec.Msg{ID: 1}); err != ErrClosed {
		t.Fatalf("send after close: want ErrClosed, got %v", err)
	}
	select {
	case err := <-reasonCh:
		if err != ErrClosed {
			t.Fatalf("OnClose reason = %v, want ErrClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OnClose not called")
	}
	if closes != 1 {
		t.Fatalf("OnClose called %d times", closes)
	}
}

// 5.6 空闲超时
func TestIdleTimeout(t *testing.T) {
	reasonCh := make(chan error, 1)
	server := New(
		WithIdleTimeout(150*time.Millisecond),
		WithHooks(Hooks{OnClose: func(s *Conn, err error) { reasonCh <- err }}),
		WithLogger(quietLogger()),
	)
	serverEnd, _ := pipePair()
	_ = attachConn(t, server, serverEnd)

	select {
	case err := <-reasonCh:
		if err != ErrIdle {
			t.Fatalf("reason = %v, want ErrIdle", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("idle close did not happen")
	}
}

// 5.7 handler panic：OnError 收到错误，连接保持
func TestPanicRecovered(t *testing.T) {
	errCh := make(chan error, 1)
	server := New(WithIdleTimeout(time.Minute), WithHooks(Hooks{
		OnError: func(s *Conn, err error) { errCh <- err },
	}), WithLogger(quietLogger()))
	tt := newTestTable(server)
	tt.Handle(1, func(s *Conn, m codec.Msg) { panic("boom") })
	tt.Handle(2, func(s *Conn, m codec.Msg) { _ = s.Send(codec.Msg{ID: 2, Body: []byte("ok")}) })

	serverEnd, testEnd := pipePair()
	_ = attachConn(t, server, serverEnd)

	msgCh := make(chan codec.Msg, 4)
	errRd := make(chan error, 1)
	go readAll(testEnd, newCodec(), msgCh, errRd)

	writeMsg(t, testEnd, newCodec(), codec.Msg{ID: 1, Body: []byte("p")})
	writeMsg(t, testEnd, newCodec(), codec.Msg{ID: 2, Body: []byte("q")})

	var errGot error
	select {
	case errGot = <-errCh:
	case <-time.After(2 * time.Second):
		t.Fatal("OnError not called")
	}
	if !strings.Contains(errGot.Error(), "boom") {
		t.Fatalf("OnError = %v", errGot)
	}
	select {
	case m := <-msgCh:
		if string(m.Body) != "ok" {
			t.Fatalf("Conn not alive: got %q", m.Body)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("message after panic lost")
	}
}

// 5.8 超大消息
func TestOversize(t *testing.T) {
	reasonCh := make(chan error, 1)
	server := New(
		WithCodec(codec.DefaultCodec(64)),
		WithHooks(Hooks{OnClose: func(s *Conn, err error) { reasonCh <- err }}),
		WithLogger(quietLogger()),
	)
	serverEnd, testEnd := pipePair()
	_ = attachConn(t, server, serverEnd)

	hdr := []byte{0, 0, 0, 100, 0, 0, 0, 1} // len=100 > 64
	if _, err := testEnd.Write(hdr); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-reasonCh:
		if err != codec.ErrTooLarge {
			t.Fatalf("reason = %v, want ErrTooLarge", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("oversize close did not happen")
	}
}

// 5.10 并发 Send × Close：nil 即已入队（-race 下验证 C4/C5）
func TestSendCloseRace(t *testing.T) {
	server := New(WithQueueSize(4096), WithIdleTimeout(time.Minute), WithLogger(quietLogger()))
	serverEnd, testEnd := pipePair()
	s := attachConn(t, server, serverEnd)

	msgCh := make(chan codec.Msg, 4096*2)
	errCh := make(chan error, 1)
	go readAll(testEnd, newCodec(), msgCh, errCh)

	const workers, per = 2, 16
	var nilSends atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < per; i++ {
				if err := s.Send(codec.Msg{ID: 1, Body: []byte("r")}); err == nil {
					nilSends.Add(1)
				}
			}
		}()
	}
	go func() { // 与发送并发地关闭
		time.Sleep(5 * time.Millisecond)
		s.Close()
	}()
	wg.Wait()

	// nil 返回的消息必须全部到达
	var delivered int64
collect:
	for {
		select {
		case <-msgCh:
			delivered++
		case err := <-errCh:
			if delivered < nilSends.Load() {
				t.Fatalf("connection closed with %d/%d delivered: %v",
					delivered, nilSends.Load(), err)
			}
			break collect
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout waiting for flushed messages (%d/%d)",
				delivered, nilSends.Load())
		}
	}
	if delivered != nilSends.Load() {
		t.Fatalf("delivered %d, want %d", delivered, nilSends.Load())
	}
}

// 5.11 半开对端限时收尾（P0）
func TestHalfOpenWriterDeadline(t *testing.T) {
	reasonCh := make(chan error, 1)
	server := New(
		shortFlush(), // 写超时/排空 150ms
		WithQueueSize(1024),
		WithIdleTimeout(time.Minute),
		WithHooks(Hooks{OnClose: func(s *Conn, err error) { reasonCh <- err }}),
		WithLogger(quietLogger()),
	)
	serverEnd, testEnd := pipePair()
	s := attachConn(t, server, serverEnd)
	_ = testEnd // 对端停止读取

	for i := 0; i < 8; i++ {
		if err := s.Send(codec.Msg{ID: 1, Body: []byte("half")}); err != nil {
			break
		}
	}

	select {
	case err := <-reasonCh:
		if err == nil {
			t.Fatal("reason must be non-nil")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("half-open close did not happen within timeout")
	}
	if err := s.Send(codec.Msg{ID: 1}); err != ErrClosed {
		t.Fatalf("send after half-open close: want ErrClosed, got %v", err)
	}
	waitSessionGoroutines(t, server) // 检查无协程泄漏
}

func waitSessionGoroutines(t *testing.T, server *Server) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		server.allConnWG.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Conn goroutines leaked")
	}
}

// Debug 消息流水：msg in/out
type msgLogCapture struct {
	mu   sync.Mutex
	recs []slog.Record
}

func (h *msgLogCapture) Enabled(context.Context, slog.Level) bool { return true }

func (h *msgLogCapture) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	h.recs = append(h.recs, r.Clone())
	h.mu.Unlock()
	return nil
}

func (h *msgLogCapture) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *msgLogCapture) WithGroup(string) slog.Handler      { return h }

// 内核在 Debug 级发消息流水（msg in/out）；ext 可用 slog.Handler 截取。
func TestMsgFlowDebugLogged(t *testing.T) {
	cap := &msgLogCapture{}
	server := New(WithLogger(slog.New(cap)))
	tt := newTestTable(server)
	tt.Handle(1, func(s *Conn, m codec.Msg) { _ = s.Send(m) })

	serverEnd, testEnd := pipePair()
	_ = attachConn(t, server, serverEnd)
	c := newCodec()
	writeMsg(t, testEnd, c, codec.Msg{ID: 1, Body: []byte("hi")})
	_ = testEnd.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := c.ReadMsg(testEnd); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		cap.mu.Lock()
		in, out := false, false
		for _, r := range cap.recs {
			if r.Level != slog.LevelDebug {
				continue
			}
			switch r.Message {
			case "msg in":
				in = true
			case "msg out":
				out = true
			}
		}
		cap.mu.Unlock()
		if in && out {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("msg in/out Debug logs missing")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Conn.Done：连接结束后立即可读（供运行期观察者等待关闭，不占钩子列表）。
func TestConnDone(t *testing.T) {
	server := New(WithLogger(quietLogger()))
	serverEnd, testEnd := net.Pipe()
	connection := attachConn(t, server, serverEnd)
	testEnd.Close()
	select {
	case <-connection.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("Done not closed after conn end")
	}
}

// TrySend：非阻塞入队；满即 ErrQueueFull、连接保持；关闭后 ErrClosed。
func TestTrySend(t *testing.T) {
	server := New(WithQueueSize(1), shortFlush(), WithLogger(quietLogger()))
	serverEnd, _ := pipePair() // 对端不读：同步写阻塞
	s := attachConn(t, server, serverEnd)

	gotFull := false
	for i := 0; i < 64; i++ {
		err := s.TrySend(codec.Msg{ID: 1, Body: []byte("x")})
		if err == ErrQueueFull {
			gotFull = true
			break
		}
		if err != nil {
			t.Fatalf("send #%d: %v (want eventually ErrQueueFull)", i, err)
		}
	}
	if !gotFull {
		t.Fatal("never got ErrQueueFull")
	}
	s.Close()
	if err := s.TrySend(codec.Msg{ID: 1}); err != ErrClosed {
		t.Fatalf("TrySend after close = %v, want ErrClosed", err)
	}
	waitSessionGoroutines(t, server)
}

// SendWait 限时：ctx 到期返回 ctx 错误，不占用阻塞预算。
func TestSendWaitDeadline(t *testing.T) {
	// 写超时 500ms：给 writer 留出阻塞窗口，避免排空预算先行关闭连接
	server := New(WithQueueSize(1), WithFlushTimeout(500*time.Millisecond), WithLogger(quietLogger()))
	serverEnd, _ := pipePair() // 对端不读：同步写阻塞
	s := attachConn(t, server, serverEnd)

	if err := s.TrySend(codec.Msg{ID: 1, Body: []byte("x")}); err != nil {
		t.Fatalf("send: %v", err)
	}
	time.Sleep(100 * time.Millisecond) // writer 挂在第一条阻塞写上

	gotFull := false
	for i := 0; i < 64; i++ {
		err := s.TrySend(codec.Msg{ID: 1, Body: []byte("x")})
		if err == ErrQueueFull {
			gotFull = true
			break
		}
		if err != nil {
			t.Fatalf("send #%d: %v (want eventually ErrQueueFull)", i, err)
		}
	}
	if !gotFull {
		t.Fatal("never got ErrQueueFull")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := s.SendWait(ctx, codec.Msg{ID: 1, Body: []byte("y")})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("SendWait = %v, want context.DeadlineExceeded", err)
	}
	waitSessionGoroutines(t, server)
}

// SendWait(nil) 等价于 Send：空余队列立即入队。
func TestSendWaitNilCtx(t *testing.T) {
	server := New(WithQueueSize(256), shortFlush(), WithLogger(quietLogger()))
	serverEnd, _ := pipePair()
	s := attachConn(t, server, serverEnd)
	defer s.Close()
	if err := s.SendWait(nil, codec.Msg{ID: 1, Body: []byte("z")}); err != nil {
		t.Fatalf("SendWait(nil) = %v", err)
	}
	waitSessionGoroutines(t, server)
}
