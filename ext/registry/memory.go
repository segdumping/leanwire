package registry

import (
	"context"
	"errors"
	"log/slog"
	"sort"
	"sync"
)

var ErrClosed = errors.New("registry: already closed")

type Option func(*Memory)

// WithLogger 默认 slog.Default()。
func WithLogger(l *slog.Logger) Option {
	return func(m *Memory) {
		if l != nil {
			m.log = l
		}
	}
}

type Memory struct {
	mu sync.Mutex

	byName map[string]map[string]Endpoint // name -> addr -> instance
	nextID int                            // watcher 分配 id

	watches map[string][]*watchState // name -> 按注册顺序的 watcher
	pending map[string]struct{}      // 待派发的 name

	ch       chan struct{}
	closedCh chan struct{}
	closed   bool

	log *slog.Logger

	wg sync.WaitGroup
}

func NewMemory(opts ...Option) *Memory {
	m := &Memory{
		byName:   make(map[string]map[string]Endpoint),
		watches:  make(map[string][]*watchState),
		pending:  make(map[string]struct{}),
		ch:       make(chan struct{}, 1),
		closedCh: make(chan struct{}),
		log:      slog.Default(),
	}
	for _, o := range opts {
		o(m)
	}
	m.wg.Add(1)
	go m.dispatchLoop()
	return m
}

type watchState struct {
	id  int
	ctx context.Context
	cb  func([]Endpoint)
}

var _ Registry = (*Memory)(nil)

func (m *Memory) Register(ctx context.Context, name string, ep Endpoint) (func(), error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if name == "" {
		return nil, errors.New("registry: empty name")
	}
	if ep.Addr == "" {
		return nil, errors.New("registry: empty addr")
	}

	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil, ErrClosed
	}
	insts, ok := m.byName[name]
	if !ok {
		insts = make(map[string]Endpoint)
		m.byName[name] = insts
	}
	insts[ep.Addr] = ep.clone()
	m.notifyLocked(name)
	m.mu.Unlock()

	// 仅槽位仍匹配才 remove，避免误删覆盖后的新实例。
	remove := func() {
		m.mu.Lock()
		if insts, ok := m.byName[name]; ok {
			if cur, ok2 := insts[ep.Addr]; ok2 && cur.Addr == ep.Addr {
				delete(insts, ep.Addr)
				m.notifyLocked(name)
			}
		}
		m.mu.Unlock()
	}

	waitCtx := make(chan struct{})
	var killOnce sync.Once
	kill := func() {
		killOnce.Do(func() {
			close(waitCtx)
			remove()
		})
	}
	go func() {
		select {
		case <-ctx.Done():
			kill()
		case <-waitCtx:
			// dereg 已生效，直接退出
		case <-m.closedCh:
			// registry 已关闭，remove 为 no-op
			kill()
		}
	}()

	return kill, nil
}

func (m *Memory) Watch(ctx context.Context, name string, cb func([]Endpoint)) error {
	if ctx == nil {
		ctx = context.Background()
	}

	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return ErrClosed
	}
	w := &watchState{id: m.nextID, ctx: ctx, cb: cb}
	m.nextID++
	m.watches[name] = append(m.watches[name], w)
	m.notifyLocked(name) // 首帧也走派发协程，保证串行单调
	m.mu.Unlock()

	go func() {
		select {
		case <-ctx.Done():
		case <-m.closedCh:
		}
		m.mu.Lock()
		ws := m.watches[name]
		for i, it := range ws {
			if it.id == w.id {
				m.watches[name] = append(ws[:i:i], ws[i+1:]...)
				break
			}
		}
		m.mu.Unlock()
	}()
	return nil
}

func (m *Memory) Close() {
	type final struct {
		ws   []*watchState
		snap []Endpoint
	}
	var finals []final

	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	m.closed = true
	m.byName = make(map[string]map[string]Endpoint) // 全部实例下线；旧句柄 remove 变 no-op
	for _, ws := range m.watches {
		if len(ws) == 0 {
			continue
		}
		finals = append(finals, final{ws: append([]*watchState(nil), ws...), snap: []Endpoint{}})
	}
	m.mu.Unlock()

	close(m.closedCh)
	close(m.ch)
	m.wg.Wait()

	for _, f := range finals {
		for _, w := range f.ws {
			select {
			case <-w.ctx.Done():
				continue
			default:
			}
			m.deliver(w, f.snap)
		}
	}
}

func (m *Memory) deliver(w *watchState, snap []Endpoint) {
	defer func() {
		if r := recover(); r != nil {
			m.log.Error("registry: watcher callback panic", "watcher", w.id, "panic", r)
		}
	}()
	if w.cb != nil {
		w.cb(snap)
	}
}

// notifyLocked 标记待派发并通知
func (m *Memory) notifyLocked(name string) {
	m.pending[name] = struct{}{}
	select {
	case m.ch <- struct{}{}:
	default:
	}
}

// snapshotLocked 生成 name 实例集的有序快照
func (m *Memory) snapshotLocked(name string) []Endpoint {
	eps := m.byName[name]
	out := make([]Endpoint, 0, len(eps))
	for _, ep := range eps {
		out = append(out, ep)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Addr < out[j].Addr })
	return out
}

func (m *Memory) dispatchLoop() {
	defer m.wg.Done()
	for range m.ch {
		m.mu.Lock()
		names := make([]string, 0, len(m.pending))
		for n := range m.pending {
			names = append(names, n)
		}
		m.pending = make(map[string]struct{})

		type job struct {
			ws   []*watchState
			snap []Endpoint
		}
		jobs := make([]job, 0, len(names))
		for _, n := range names {
			jobs = append(jobs, job{
				ws:   append([]*watchState(nil), m.watches[n]...),
				snap: m.snapshotLocked(n),
			})
		}
		m.mu.Unlock()

		for _, j := range jobs {
			if len(j.ws) == 0 {
				continue
			}
			for _, w := range j.ws {
				select {
				case <-w.ctx.Done():
					continue
				default:
				}
				m.deliver(w, j.snap)
			}
		}
	}
}
