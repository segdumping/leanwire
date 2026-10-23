package breaker

import (
	"context"
	"errors"
	"sync"
	"time"
)

const (
	defaultThreshold = 5
	defaultCooldown  = 10 * time.Second
)

// ErrOpen 熔断拒绝。
var ErrOpen = errors.New("breaker: circuit open")

// ErrPanic 调用 panic 的记账哨兵。
var ErrPanic = errors.New("breaker: call panicked")

type Breaker struct {
	mu        sync.Mutex
	threshold int
	cooldown  time.Duration
	fails     int
	until     time.Time
	probing   bool
}

type Options struct {
	threshold int
	cooldown  time.Duration
}

type Option func(*Options)

// WithFailThreshold 连续失败 N 次断开，缺省 5。
func WithFailThreshold(n int) Option {
	return func(o *Options) {
		if n > 0 {
			o.threshold = n
		}
	}
}

// WithCooldown 断开态冷却时长，缺省 10s。
func WithCooldown(d time.Duration) Option {
	return func(o *Options) {
		if d > 0 {
			o.cooldown = d
		}
	}
}

func New(opts ...Option) *Breaker {
	o := Options{threshold: defaultThreshold, cooldown: defaultCooldown}
	for _, f := range opts {
		f(&o)
	}
	return &Breaker{threshold: o.threshold, cooldown: o.cooldown}
}

// Allow 报告是否放行；冷却到期后只放行一颗半开探针。
func (b *Breaker) Allow() bool {
	ok, _ := b.allow()
	return ok
}

func (b *Breaker) allow() (bool, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.probing {
		return false, false
	}
	now := time.Now()
	if b.until.IsZero() || now.After(b.until) {
		probe := !b.until.IsZero()
		if probe {
			b.probing = true
		}
		return true, probe
	}
	return false, false
}

// Record 上报调用结果。
func (b *Breaker) Record(err error) { b.record(err, true) }

func (b *Breaker) record(err error, probe bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if probe {
		b.probing = false
	}
	if err == nil {
		b.fails = 0
		b.until = time.Time{}
		return
	}
	b.fails++
	if b.fails >= b.threshold {
		b.until = time.Now().Add(b.cooldown)
	}
}

// Do 给 fn 包熔断；断开态返回 ErrOpen 且不执行 fn。
func (b *Breaker) Do(ctx context.Context, fn func(context.Context) error) (err error) {
	ok, probe := b.allow()
	if !ok {
		return ErrOpen
	}
	defer func() {
		if r := recover(); r != nil {
			b.record(ErrPanic, probe)
			panic(r)
		}
	}()
	err = fn(ctx)
	b.record(err, probe)
	return err
}

// WrapCall 给同步请求函数包熔断。
func (b *Breaker) WrapCall(call func(context.Context, []byte) ([]byte, error)) func(context.Context, []byte) ([]byte, error) {
	return func(ctx context.Context, req []byte) (resp []byte, err error) {
		ok, probe := b.allow()
		if !ok {
			return nil, ErrOpen
		}
		defer func() {
			if r := recover(); r != nil {
				b.record(ErrPanic, probe)
				panic(r)
			}
		}()
		resp, err = call(ctx, req)
		b.record(err, probe)
		return resp, err
	}
}
