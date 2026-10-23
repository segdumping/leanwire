package heartbeat

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/segdumping/leanwire/codec"
	"github.com/segdumping/leanwire/server"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// 按连接周期发送心跳，Stop 后停止。
func TestHeartbeat(t *testing.T) {
	a := server.New(server.WithLogger(quietLogger()))
	hb := New(a, 30*time.Millisecond) // 必须在第一个 Connect 之前挂接
	defer hb.Stop()

	serverEnd, testEnd := net.Pipe()
	if _, err := a.Connect(serverEnd); err != nil {
		t.Fatal(err)
	}

	c := codec.DefaultCodec(64 * 1024)
	for i := 0; i < 2; i++ { // 至少两个周期
		m, err := c.ReadMsg(testEnd)
		if err != nil {
			t.Fatalf("read #%d: %v", i, err)
		}
		if m.ID != DefaultID {
			t.Fatalf("msg id %d, want %d", m.ID, DefaultID)
		}
	}

	// Stop 之后不再有心跳
	hb.Stop()
	_ = testEnd.SetReadDeadline(time.Now().Add(120 * time.Millisecond))
	if _, err := c.ReadMsg(testEnd); err == nil {
		t.Fatal("heartbeat still sent after Stop")
	}
}

// 连接被对端关闭后心跳协程自行退出。
func TestHeartbeatFollowsSessionDeath(t *testing.T) {
	a := server.New(server.WithLogger(quietLogger()))
	hb := New(a, 30*time.Millisecond)
	defer hb.Stop()

	serverEnd, testEnd := net.Pipe()
	if _, err := a.Connect(serverEnd); err != nil {
		t.Fatal(err)
	}

	// 先确认至少收到一次心跳
	c := codec.DefaultCodec(64 * 1024)
	_ = testEnd.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := c.ReadMsg(testEnd); err != nil {
		t.Fatalf("first heartbeat missing: %v", err)
	}

	// 对端关闭
	testEnd.Close()

	// Stop + Shutdown 收尾（泄漏观察靠 -race）
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := a.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
}
