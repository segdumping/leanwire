package trace

import (
	crand "crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/segdumping/leanwire/codec"
	"github.com/segdumping/leanwire/server"
)

const idLen = 8

type ID [idLen]byte

var (
	rngMu sync.Mutex
	rng   = func() *rand.Rand {
		var seed [32]byte
		if _, err := crand.Read(seed[:]); err != nil {
			panic("trace: seed rand: " + err.Error())
		}
		return rand.New(rand.NewChaCha8(seed))
	}()
)

func NewID() (id ID) {
	rngMu.Lock()
	v := rng.Uint64()
	rngMu.Unlock()
	binary.BigEndian.PutUint64(id[:], v)
	return id
}

func (id ID) String() string { return hex.EncodeToString(id[:]) }

// ErrShortTrace body 不足 traceID 前缀。
var ErrShortTrace = errors.New("trace: body missing traceID prefix")

func Pack(id ID, body []byte) []byte {
	out := make([]byte, idLen+len(body))
	copy(out, id[:])
	copy(out[idLen:], body)
	return out
}

func Unpack(body []byte) (ID, []byte, error) {
	if len(body) < idLen {
		return ID{}, nil, ErrShortTrace
	}
	var id ID
	copy(id[:], body)
	return id, body[idLen:], nil
}

// Handle 包装 handler，拆出 traceID 并记录耗时。
func Handle(log *slog.Logger, fn server.HandlerFunc) server.HandlerFunc {
	if log == nil {
		log = slog.Default()
	}
	return func(s *server.Conn, m codec.Msg) {
		start := time.Now()
		body := m.Body
		tid := "-"
		if id, rest, err := Unpack(body); err == nil {
			body, tid = rest, id.String()
		}
		fn(s, codec.Msg{ID: m.ID, Body: body})
		log.Debug("trace: msg handle",
			"conn", s.ID(), "msgID", m.ID, "trace", tid, "n", len(body), "cost", time.Since(start))
	}
}
