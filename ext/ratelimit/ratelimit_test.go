package ratelimit

// 测试验证：令牌桶补足/丢保护、连接隔离、全局桶共享、OnClose 回收。

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/segdumping/leanwire/codec"
	"github.com/segdumping/leanwire/ext/router"
	"github.com/segdumping/leanwire/server"
)

// capLog 收集日志消息文本，供断言。
type capLog struct {
	mu     sync.Mutex
	buffer strings.Builder
}

func (h *capLog) Enabled(context.Context, slog.Level) bool { return true }

func (h *capLog) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	fmt.Fprintf(&h.buffer, "%s ", r.Message)
	r.Attrs(func(a slog.Attr) bool {
		fmt.Fprintf(&h.buffer, "%s=%v ", a.Key, a.Value)
		return true
	})
	h.buffer.WriteByte('\n')
	return nil
}

func (h *capLog) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *capLog) WithGroup(string) slog.Handler      { return h }

func (h *capLog) String() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.buffer.String()
}

func TestBucketRefillAndDrop(t *testing.T) {
	b := &bucket{rate: 50, burst: 2, tokens: 2, last: time.Now()} // 每秒补 50，容量 2
	t0 := time.Now()
	if !b.allow(t0) || !b.allow(t0) {
		t.Fatal("full bucket allows burst")
	}
	if b.allow(t0) {
		t.Fatal("empty bucket drops")
	}
	t1 := t0.Add(40 * time.Millisecond) // 40ms × 50/s = 恰好 2 tokens
	if !b.allow(t1) || !b.allow(t1) {
		t.Fatal("refill restores burst")
	}
	if b.allow(t1) {
		t.Fatal("burst exhausted again")
	}
}

// 连接级：超限丢弃，handler 不执行

func TestWrapPerSessionDrop(t *testing.T) {
	log := &capLog{}
	testServer := server.New(server.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
		server.WithFlushTimeout(150*time.Millisecond))
	l := New(testServer, WithQPS(1), WithBurst(1), WithLogger(slog.New(log)))
	var got atomic.Int64
	routeTable := router.Bind(testServer)
	routeTable.Handle(1, l.Wrap(func(*server.Conn, codec.Msg) { got.Add(1) }))

	serverEnd, testEnd := net.Pipe()
	if _, err := testServer.Connect(serverEnd); err != nil {
		t.Fatal(err)
	}
	c := codec.DefaultCodec(64 * 1024)
	for i := 0; i < 5; i++ { // 桶容 1、qps 1/s：首条执行，余 4 条必丢
		if err := c.WriteMsg(testEnd, codec.Msg{ID: 1, Body: []byte("x")}); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(100 * time.Millisecond)
	if got.Load() != 1 {
		t.Fatalf("handler executions = %d, want 1", got.Load())
	}
	if !strings.Contains(log.String(), "ratelimit: drop") {
		t.Fatal("drops not logged")
	}
	_ = testServer.Shutdown(context.Background())
}

// 全局桶：跨连接共享配额

func TestGlobalBucketShared(t *testing.T) {
	log := &capLog{}
	testServer := server.New(server.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
		server.WithFlushTimeout(150*time.Millisecond))
	l := New(testServer, WithQPS(0) /*禁连接级*/, WithGlobalQPS(1), WithLogger(slog.New(log)))
	var got atomic.Int64
	routeTable := router.Bind(testServer)
	routeTable.Handle(1, l.Wrap(func(*server.Conn, codec.Msg) { got.Add(1) }))

	c := codec.DefaultCodec(64 * 1024)
	for i := 0; i < 2; i++ { // 两个连接
		serverEnd, testEnd := net.Pipe()
		if _, err := testServer.Connect(serverEnd); err != nil {
			t.Fatal(err)
		}
		for j := 0; j < 2; j++ {
			if err := c.WriteMsg(testEnd, codec.Msg{ID: 1, Body: []byte("x")}); err != nil {
				t.Fatal(err)
			}
		}
	}
	time.Sleep(100 * time.Millisecond)
	if got.Load() != 1 { // 全局桶容 1：仅第一条执行
		t.Fatalf("handler executions = %d, want 1 (global quota)", got.Load())
	}
	if !strings.Contains(log.String(), "reason=global") {
		t.Fatal("global drop reason not logged")
	}
	_ = testServer.Shutdown(context.Background())
}

func TestOnCloseEvictsBucket(t *testing.T) {
	testServer := server.New(server.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
		server.WithFlushTimeout(150*time.Millisecond))
	l := New(testServer, WithQPS(100))
	routeTable := router.Bind(testServer)
	routeTable.Handle(1, l.Wrap(func(*server.Conn, codec.Msg) {}))

	serverEnd, testEnd := net.Pipe()
	if _, err := testServer.Connect(serverEnd); err != nil {
		t.Fatal(err)
	}
	c := codec.DefaultCodec(64 * 1024)
	if err := c.WriteMsg(testEnd, codec.Msg{ID: 1}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) { // 首条消息触发建桶
		if bucketCount(l) == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	_ = testEnd.Close()
	deadline = time.Now().Add(2 * time.Second)
	for {
		n := bucketCount(l)
		if n == 0 { // 连接结束 → OnClose 回收
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("conn bucket not evicted: %d", n)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// bucketCount 统计当前连接桶数量（sync.Map 无 len）。
func bucketCount(l *Limiter) int {
	n := 0
	l.buckets.Range(func(any, any) bool {
		n++
		return true
	})
	return n
}
