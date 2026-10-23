package breaker

// 测试验证：连败断开、断开快速失败、半开探针收尾/重开、连败重置与 WrapCall。

import (
	"context"
	"errors"
	"testing"
	"time"
)

var errBoom = errors.New("boom")

func TestOpenAfterConsecutiveFailures(t *testing.T) {
	b := New(WithFailThreshold(3), WithCooldown(50*time.Millisecond))
	calls := 0
	fn := func(context.Context) error { calls++; return errBoom }

	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if err := b.Do(ctx, fn); err != errBoom {
			t.Fatalf("call %d: want boom, got %v", i, err)
		}
	}
	if b.Allow() {
		t.Fatal("threshold reached should be open")
	}
	if err := b.Do(ctx, fn); err != ErrOpen {
		t.Fatalf("open state: want ErrOpen, got %v", err)
	}
	if calls != 3 {
		t.Fatalf("fn must not run while open: %d calls", calls)
	}
}

func TestSuccessResetsStreak(t *testing.T) {
	b := New(WithFailThreshold(3))
	for i := 0; i < 2; i++ {
		b.Record(errBoom)
	}
	b.Record(nil) // 成功重置
	for i := 0; i < 2; i++ {
		b.Record(errBoom)
	}
	if !b.Allow() {
		t.Fatal("streak reset by success")
	}
	b.Record(errBoom) // 新一轮第 3 次
	if b.Allow() {
		t.Fatal("3 consecutive failures should open")
	}
}

func TestHalfOpenRecovers(t *testing.T) {
	b := New(WithFailThreshold(2), WithCooldown(50*time.Millisecond))
	fn := func(context.Context) error { return errBoom }
	ctx := context.Background()
	_ = b.Do(ctx, fn)
	_ = b.Do(ctx, fn) // 达阈值 → open
	if b.Allow() {
		t.Fatal("should be open")
	}

	time.Sleep(60 * time.Millisecond) // 冷却到 → 半开放行探针
	ok := 0
	if err := b.Do(ctx, func(context.Context) error { ok++; return nil }); err != nil || ok != 1 {
		t.Fatalf("half-open probe: err=%v ok=%d", err, ok)
	}
	if !b.Allow() {
		t.Fatal("success should close circuit")
	}
}

func TestHalfOpenProbeFailureReopens(t *testing.T) {
	b := New(WithFailThreshold(2), WithCooldown(50*time.Millisecond))
	fn := func(context.Context) error { return errBoom }
	ctx := context.Background()
	_ = b.Do(ctx, fn)
	_ = b.Do(ctx, fn) // open

	time.Sleep(60 * time.Millisecond) // 半开
	if err := b.Do(ctx, fn); err != errBoom {
		t.Fatalf("probe must run: %v", err)
	}
	if b.Allow() {
		t.Fatal("probe failure should reopen")
	}
	if err := b.Do(ctx, fn); err != ErrOpen {
		t.Fatalf("reopened: want ErrOpen, got %v", err)
	}
}

// panic 也要记账：探针 panic 后重开熔断，且探针名额必须释放（否则永久卡死）。
func TestPanicRecordsAndReleasesProbe(t *testing.T) {
	b := New(WithFailThreshold(1), WithCooldown(40*time.Millisecond))
	ctx := context.Background()

	_ = b.Do(ctx, func(context.Context) error { return errBoom }) // open
	if b.Allow() {
		t.Fatal("should be open")
	}

	time.Sleep(50 * time.Millisecond) // 半开
	var got any
	func() {
		defer func() { got = recover() }()
		_ = b.Do(ctx, func(context.Context) error { panic("boom") })
	}()
	if got == nil {
		t.Fatal("panic must propagate to caller")
	}
	if b.Allow() {
		t.Fatal("probe panic should reopen circuit")
	}

	time.Sleep(50 * time.Millisecond)
	if !b.Allow() {
		t.Fatal("probe slot leaked by panic: circuit stuck open")
	}
}

// 普通在途调用迟到的 Record 不得释放半开探针名额（只允许探针自己收尾）。
func TestLateRecordDoesNotReleaseProbe(t *testing.T) {
	b := New(WithFailThreshold(2), WithCooldown(40*time.Millisecond))
	ctx := context.Background()

	inFlight := make(chan struct{})
	release := make(chan struct{})
	lateDone := make(chan struct{})
	go func() {
		defer close(lateDone)
		_ = b.Do(ctx, func(context.Context) error {
			close(inFlight)
			<-release
			return errBoom // 迟到失败
		})
	}()
	<-inFlight

	// 再连败两次 → open（迟到调用仍挂在途）
	_ = b.Do(ctx, func(context.Context) error { return errBoom })
	_ = b.Do(ctx, func(context.Context) error { return errBoom })
	if b.Allow() {
		t.Fatal("should be open")
	}

	time.Sleep(50 * time.Millisecond) // 半开
	probeStarted := make(chan struct{})
	probeRelease := make(chan struct{})
	probeDone := make(chan struct{})
	go func() {
		defer close(probeDone)
		_ = b.Do(ctx, func(context.Context) error {
			close(probeStarted)
			<-probeRelease
			return nil
		})
	}()
	<-probeStarted // 探针名额已占用

	close(release) // 迟到调用返回并 Record（非探针）
	<-lateDone
	if err := b.Do(ctx, func(context.Context) error { return nil }); err != ErrOpen {
		t.Fatalf("probe slot released by late non-probe record: %v", err)
	}

	close(probeRelease) // 探针成功收尾
	<-probeDone
	if !b.Allow() {
		t.Fatal("probe success should close circuit")
	}
}

func TestWrapCall(t *testing.T) {
	b := New(WithFailThreshold(1), WithCooldown(time.Minute))
	n := 0
	call := b.WrapCall(func(context.Context, []byte) ([]byte, error) {
		n++
		return []byte("ok"), nil
	})

	resp, err := call(context.Background(), []byte("q"))
	if err != nil || string(resp) != "ok" {
		t.Fatalf("first call: %v %q", err, resp)
	}

	b.Record(errBoom) // 再败一次（阈 1）→ open
	if _, err := call(context.Background(), nil); err != ErrOpen {
		t.Fatalf("open: want ErrOpen, got %v", err)
	}
	if n != 1 {
		t.Fatalf("underlying must not run while open: %d", n)
	}
}
