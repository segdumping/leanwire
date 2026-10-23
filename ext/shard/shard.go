// Package shard 提供按 key 分片串行执行的工作池。
//
// TryPost 队列满或已停止时返回 false，由调用方决定如何处理。
package shard

import (
	"context"
	"log/slog"
	"sync"
)

// defaultQueueCap 缺省的分片队列容量（WithQueueCap 覆盖）。
const defaultQueueCap = 1024

type job struct {
	key uint64
	fn  func()
}

// Pool 是分片执行池。
type Pool struct {
	queueCap int
	queues   []chan job

	// mu 串行化 TryPost 的投递检查与 Drain 的 close(queues)：两者无互斥会
	// 产生 send-on-closed panic（closing 判断与 close 之间的窗口）。
	mu      sync.RWMutex
	closing bool
	wg      sync.WaitGroup

	onPanic func(key uint64, r any)
	log     *slog.Logger
}

type Option func(*Pool)

// WithQueueCap 设置分片队列容量，默认 1024。
func WithQueueCap(n int) Option { return func(p *Pool) { p.queueCap = n } }

// WithPanicHook 注册 panic 钩子（缺省仅记日志）。
func WithPanicHook(f func(key uint64, r any)) Option { return func(p *Pool) { p.onPanic = f } }

// WithLogger 设置日志器，默认 slog.Default()。
func WithLogger(l *slog.Logger) Option { return func(p *Pool) { p.log = l } }

// New 启动 n 个分片 worker（n<=0 退化为 1）。
func New(n int, opts ...Option) *Pool {
	if n <= 0 {
		n = 1
	}
	p := &Pool{queueCap: defaultQueueCap, log: slog.Default()}
	for _, o := range opts {
		o(p)
	}
	if p.queueCap <= 0 {
		p.queueCap = defaultQueueCap
	}
	if p.log == nil {
		p.log = slog.Default()
	}
	p.queues = make([]chan job, n)
	p.wg.Add(n)
	for i := range p.queues {
		p.queues[i] = make(chan job, p.queueCap)
		go p.worker(i)
	}
	return p
}

// TryPost 非阻塞投递，同一 key 严格串行；返回 false = 队列满或已停止。
func (p *Pool) TryPost(key uint64, task func()) bool {
	if task == nil {
		return false
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closing {
		return false
	}
	select {
	case p.queues[key%uint64(len(p.queues))] <- job{key: key, fn: task}:
		return true
	default:
		return false
	}
}

// Post 阻塞投递，同一 key 严格串行；返回 false = 已停止。
// 不要在 worker 任务内对同一个已满队列调用 Post，否则会自锁。
func (p *Pool) Post(key uint64, task func()) bool {
	if task == nil {
		return false
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closing {
		return false
	}
	p.queues[key%uint64(len(p.queues))] <- job{key: key, fn: task}
	return true
}

// Drain 停止接收并等全部在途任务完成；ctx 到期返回其 Err（任务仍会跑完）。
func (p *Pool) Drain(ctx context.Context) error {
	p.mu.Lock()
	if !p.closing {
		p.closing = true
		for _, q := range p.queues {
			close(q)
		}
	}
	p.mu.Unlock()
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// SliceIndex 取 key 对应的分片下标，便于观测。
func (p *Pool) SliceIndex(key uint64) int {
	return int(key % uint64(len(p.queues)))
}

func (p *Pool) worker(i int) {
	defer p.wg.Done()
	for j := range p.queues[i] {
		p.run(j)
	}
}

func (p *Pool) run(j job) {
	defer func() {
		if r := recover(); r != nil {
			if p.onPanic != nil {
				p.onPanic(j.key, r)
			}
			p.log.Error("shard task panic", "key", j.key, "panic", r)
		}
	}()
	j.fn()
}
