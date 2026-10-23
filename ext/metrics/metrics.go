package metrics

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"expvar"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/segdumping/leanwire/codec"
	"github.com/segdumping/leanwire/server"
)

type Metrics struct {
	name string

	opened atomic.Int64
	closed atomic.Int64
	errors atomic.Int64
	live   atomic.Int64

	reasonMu sync.RWMutex
	reasons  map[string]*atomic.Int64
}

type Option func(*Metrics)

func New(appServer *server.Server, name string, opts ...Option) *Metrics {
	m := &Metrics{name: name, reasons: make(map[string]*atomic.Int64)}
	for _, o := range opts {
		o(m)
	}
	appServer.AppendHooks(server.Hooks{
		OnOpen: func(*server.Conn) {
			m.opened.Add(1)
			m.live.Add(1)
		},
		OnClose: func(_ *server.Conn, err error) {
			m.closed.Add(1)
			m.live.Add(-1)
			m.bump(m.reason(err))
		},
		OnError: func(*server.Conn, error) {
			m.errors.Add(1)
		},
	})
	m.publish()
	return m
}

type Stats struct {
	Opened int64 `json:"opened"`
	Closed int64 `json:"closed"`
	Errors int64 `json:"errors"`
	Live   int64 `json:"live"`

	ByCloseReason map[string]int64 `json:"by_close_reason"`
}

func (m *Metrics) Stats() Stats {
	s := Stats{
		Opened: m.opened.Load(),
		Closed: m.closed.Load(),
		Errors: m.errors.Load(),
		Live:   m.live.Load(),
	}
	m.reasonMu.RLock()
	defer m.reasonMu.RUnlock()
	s.ByCloseReason = make(map[string]int64, len(m.reasons))
	for k, v := range m.reasons {
		s.ByCloseReason[k] = v.Load()
	}
	return s
}

func (m *Metrics) bump(reason string) {
	if reason == "" {
		reason = "unknown"
	}
	m.reasonMu.Lock()
	defer m.reasonMu.Unlock()
	c, ok := m.reasons[reason]
	if !ok {
		c = new(atomic.Int64)
		m.reasons[reason] = c
	}
	c.Add(1)
}

func (m *Metrics) reason(err error) string {
	if err == nil {
		return ""
	}
	switch {
	case errors.Is(err, io.EOF):
		return "eof"
	case errors.Is(err, io.ErrUnexpectedEOF):
		return "unexpected_eof"
	case errors.Is(err, server.ErrIdle):
		return "idle"
	case errors.Is(err, server.ErrQueueFull):
		return "queue_full"
	case errors.Is(err, server.ErrClosed):
		return "closed"
	case errors.Is(err, codec.ErrTooLarge):
		return "too_large"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return "timeout"
	}
	return "other"
}

type expvarVar struct{ m *Metrics }

func (v expvarVar) String() string {
	b, err := json.Marshal(v.m.Stats())
	if err != nil {
		return "{}"
	}
	return string(b)
}

func (m *Metrics) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_ = m.WritePrometheus(w)
	})
}

// WritePrometheus 以 Prometheus 文本格式输出计数。
func (m *Metrics) WritePrometheus(w io.Writer) error {
	p := m.promPrefix()
	stats := m.Stats()

	bw := bufio.NewWriter(w)
	line := func(format string, args ...any) {
		_, _ = fmt.Fprintf(bw, format, args...)
		_ = bw.WriteByte('\n')
	}

	line("# HELP %s_conns_opened_total Conns opened since start.", p)
	line("# TYPE %s_conns_opened_total counter", p)
	line("%s_conns_opened_total %d", p, stats.Opened)

	line("# HELP %s_conns_closed_total Conns closed since start.", p)
	line("# TYPE %s_conns_closed_total counter", p)
	line("%s_conns_closed_total %d", p, stats.Closed)

	line("# HELP %s_conns_live Currently open conns.", p)
	line("# TYPE %s_conns_live gauge", p)
	line("%s_conns_live %d", p, stats.Live)

	line("# HELP %s_errors_total Non-fatal errors observed (OnError).", p)
	line("# TYPE %s_errors_total counter", p)
	line("%s_errors_total %d", p, stats.Errors)

	line("# HELP %s_conn_close_reason_total Conn closes by stable reason bucket.", p)
	line("# TYPE %s_conn_close_reason_total counter", p)
	reasons := make([]string, 0, len(stats.ByCloseReason))
	for k := range stats.ByCloseReason {
		reasons = append(reasons, k)
	}
	sort.Strings(reasons)
	for _, k := range reasons {
		line("%s_conn_close_reason_total{reason=%s} %d", p, m.escapeLabel(k), stats.ByCloseReason[k])
	}
	return bw.Flush()
}

func (m *Metrics) escapeLabel(s string) string {
	var b strings.Builder
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '\\':
			b.WriteString(`\\`)
		case '"':
			b.WriteString(`\"`)
		case '\n':
			b.WriteString(`\n`)
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte('"')
	return b.String()
}

func (m *Metrics) promPrefix() string {
	var b strings.Builder
	for i, r := range m.name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r == '_', r == ':':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			if i == 0 {
				b.WriteByte('_')
			}
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	if b.Len() == 0 {
		return "leanwire"
	}
	return b.String()
}

var publishMu sync.Mutex

func (m *Metrics) publish() {
	publishMu.Lock()
	defer publishMu.Unlock()
	found := false
	expvar.Do(func(kv expvar.KeyValue) {
		if kv.Key == m.name {
			found = true
		}
	})
	if !found {
		expvar.Publish(m.name, expvarVar{m: m})
	}
}
