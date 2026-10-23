package metrics

import (
	"bytes"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/segdumping/leanwire/codec"
	"github.com/segdumping/leanwire/server"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestSessionAndReasonCounters(t *testing.T) {
	testServer := server.New(server.WithLogger(quietLogger()))
	m := New(testServer, "test.metrics.a")

	serverEnd, testEnd1 := net.Pipe()
	if _, err := testServer.Connect(serverEnd); err != nil {
		t.Fatal(err)
	}
	_, testEnd2 := net.Pipe()
	if _, err := testServer.Connect(testEnd2); err != nil {
		t.Fatal(err)
	}

	stats := pollStats(t, m, func(s Stats) bool { return s.Opened == 2 && s.Live == 2 })
	if stats.Opened != 2 || stats.Live != 2 {
		t.Fatalf("after connect: %+v", stats)
	}

	// 对端 EOF 关闭 → 原因 eof
	testEnd1.Close()

	deadline := time.Now().Add(2 * time.Second)
	for m.Stats().Closed < 1 {
		if time.Now().After(deadline) {
			t.Fatal("close not counted")
		}
		time.Sleep(10 * time.Millisecond)
	}

	stats = m.Stats()
	if stats.Live != 1 {
		t.Fatalf("live = %d, want 1", stats.Live)
	}
	foundEOF := false
	for k := range stats.ByCloseReason {
		if k == "eof" {
			foundEOF = true
		}
	}
	if !foundEOF {
		t.Fatalf("reason not recorded: %+v", stats.ByCloseReason)
	}

	// 主动关闭 → 原因 closed
	server2End, test2End := net.Pipe()
	s2, err := testServer.Connect(server2End)
	if err != nil {
		t.Fatal(err)
	}
	c := codec.DefaultCodec(64 * 1024)
	go func() {
		for {
			if _, err := c.ReadMsg(test2End); err != nil {
				return // 服务端关闭连接
			}
		}
	}()
	if err := s2.Send(codec.Msg{ID: 1}); err != nil {
		t.Fatal(err)
	}
	s2.Close()
	deadline = time.Now().Add(2 * time.Second)
	for m.Stats().Closed < 2 {
		if time.Now().After(deadline) {
			t.Fatal("second close not counted")
		}
		time.Sleep(10 * time.Millisecond)
	}
	foundClosed := false
	for k := range m.Stats().ByCloseReason {
		if k == "closed" {
			foundClosed = true
		}
	}
	if !foundClosed {
		t.Fatalf("ErrClosed reason not recorded: %+v", m.Stats().ByCloseReason)
	}
}

// 分桶按稳定错误值，不随临时端口增长。
func TestReasonBucketingIsBounded(t *testing.T) {
	m := &Metrics{reasons: make(map[string]*atomic.Int64)}
	// 两个不同端口的 reset 错误须归入同一桶。
	m.bump(m.reason(&net.OpError{Op: "read", Net: "tcp",
		Addr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 53124},
		Err:  errors.New("connection reset by peer")}))
	m.bump(m.reason(&net.OpError{Op: "read", Net: "tcp",
		Addr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 53125},
		Err:  errors.New("connection reset by peer")}))
	m.bump(m.reason(io.EOF))
	m.bump(m.reason(server.ErrQueueFull))

	stats := m.Stats()
	if len(stats.ByCloseReason) != 3 {
		t.Fatalf("expected 3 bounded buckets, got %d: %+v", len(stats.ByCloseReason), stats.ByCloseReason)
	}
	if stats.ByCloseReason["other"] != 2 {
		t.Fatalf("reset errors should bucket as other: %+v", stats.ByCloseReason)
	}
	if stats.ByCloseReason["eof"] != 1 || stats.ByCloseReason["queue_full"] != 1 {
		t.Fatalf("named buckets wrong: %+v", stats.ByCloseReason)
	}
}

// TestPrometheusExport 校验格式、转义与确定性。
func TestPrometheusExport(t *testing.T) {
	m := &Metrics{name: "service.test", reasons: make(map[string]*atomic.Int64)}
	m.opened.Add(3)
	m.closed.Add(2)
	m.live.Add(1)
	m.errors.Add(4)
	m.bump("") // → "unknown"
	m.bump("eof")
	m.bump("eof")
	m.bump("we\"ird\\\n") // 转义路径

	var buffer bytes.Buffer
	if err := m.WritePrometheus(&buffer); err != nil {
		t.Fatal(err)
	}
	out := buffer.String()
	for _, want := range []string{
		"# TYPE service_test_conns_opened_total counter",
		"service_test_conns_opened_total 3",
		"service_test_conns_closed_total 2",
		"service_test_conns_live 1",
		"service_test_errors_total 4",
		`service_test_conn_close_reason_total{reason="eof"} 2`,
		`service_test_conn_close_reason_total{reason="unknown"} 1`,
		`service_test_conn_close_reason_total{reason="we\"ird\\\n"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q in:\n%s", want, out)
		}
	}

	// 两次输出须逐字节一致
	var buf2 bytes.Buffer
	if err := m.WritePrometheus(&buf2); err != nil {
		t.Fatal(err)
	}
	if buffer.String() != buf2.String() {
		t.Fatal("output not deterministic")
	}

	// 名称规范化边界
	if p := (&Metrics{name: "9abc"}).promPrefix(); p != "_9abc" {
		t.Fatalf("promPrefix(9abc) = %q, want _9abc", p)
	}
	if p := (&Metrics{}).promPrefix(); p != "leanwire" {
		t.Fatalf("promPrefix(empty) = %q, want leanwire", p)
	}
	if p := (&Metrics{name: "a-b.c"}).promPrefix(); p != "a_b_c" {
		t.Fatalf("promPrefix(a-b.c) = %q, want a_b_c", p)
	}
}

// pollStats 轮询直到条件满足或超时（OnOpen 异步触发）。
func pollStats(t *testing.T, m *Metrics, cond func(Stats) bool) Stats {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		s := m.Stats()
		if cond(s) {
			return s
		}
		if time.Now().After(deadline) {
			return s
		}
		time.Sleep(10 * time.Millisecond)
	}
}
