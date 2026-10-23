package etcd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/segdumping/leanwire/ext/registry"
)

const (
	defaultPrefix      = "/leanwire/registry"
	defaultLeaseTTL    = 15 * time.Second
	retryBase          = time.Second
	retryMax           = 10 * time.Second
	revokeTimeout      = 2 * time.Second
	defaultDialTimeout = 3 * time.Second
)

type config struct {
	prefix      string
	leaseTTL    time.Duration
	log         *slog.Logger
	dialTout    time.Duration
	client      *clientv3.Client
	clientOwned bool
}

type Option func(*config)

// WithPrefix 设置根前缀。
func WithPrefix(p string) Option {
	return func(c *config) {
		p = strings.TrimRight(strings.TrimSpace(p), "/")
		if p != "" {
			c.prefix = p
		}
	}
}

// WithLeaseTTL 设置实例租约 TTL。
func WithLeaseTTL(d time.Duration) Option {
	return func(c *config) {
		if d > 0 {
			c.leaseTTL = d
		}
	}
}

// WithLogger 设置日志器。
func WithLogger(l *slog.Logger) Option {
	return func(c *config) {
		if l != nil {
			c.log = l
		}
	}
}

// WithDialTimeout 设置拨号超时。
func WithDialTimeout(d time.Duration) Option {
	return func(c *config) {
		if d > 0 {
			c.dialTout = d
		}
	}
}

// WithClient 注入既有 etcd 客户端。
func WithClient(client *clientv3.Client) Option {
	return func(c *config) { c.client = client }
}

type Registry struct {
	client *clientv3.Client
	conf   config
	prefix string
	log    *slog.Logger

	mu     sync.Mutex
	closed bool
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	closeOnce sync.Once
}

var _ registry.Registry = (*Registry)(nil)

func New(endpoints []string, opts ...Option) (*Registry, error) {
	conf := config{
		prefix:   defaultPrefix,
		leaseTTL: defaultLeaseTTL,
		log:      slog.Default(),
		dialTout: defaultDialTimeout,
	}
	for _, o := range opts {
		o(&conf)
	}
	if conf.log == nil {
		conf.log = slog.Default()
	}

	client := conf.client
	if client == nil {
		if len(endpoints) == 0 {
			return nil, errors.New("etcd registry: no endpoints")
		}
		var err error
		client, err = clientv3.New(clientv3.Config{
			Endpoints:   endpoints,
			DialTimeout: conf.dialTout,
		})
		if err != nil {
			return nil, fmt.Errorf("etcd registry: connect: %w", err)
		}
		conf.clientOwned = true
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &Registry{
		client: client,
		conf:   conf,
		prefix: conf.prefix,
		log:    conf.log,
		ctx:    ctx,
		cancel: cancel,
	}, nil
}

func (r *Registry) kvPrefix(name string) string { return r.prefix + "/" + name + "/" }

func (r *Registry) validateName(name string) error {
	if name == "" {
		return errors.New("etcd registry: empty name")
	}
	if strings.Contains(name, "/") {
		return errors.New("etcd registry: name must not contain '/'")
	}
	return nil
}

func (r *Registry) validateAddr(addr string) error {
	if addr == "" {
		return errors.New("etcd registry: empty addr")
	}
	if strings.Contains(addr, "/") {
		return errors.New("etcd registry: addr must not contain '/'")
	}
	return nil
}

func (r *Registry) parseKV(prefix string, key, val []byte) (string, registry.Endpoint, bool) {
	addr := strings.TrimPrefix(string(key), prefix)
	if addr == "" {
		return "", registry.Endpoint{}, false
	}
	var endpoint registry.Endpoint
	if err := json.Unmarshal(val, &endpoint); err != nil {
		return "", registry.Endpoint{}, false
	}
	endpoint.Addr = addr
	return addr, endpoint, true
}

func (r *Registry) snapshotOf(m map[string]registry.Endpoint) []registry.Endpoint {
	out := make([]registry.Endpoint, 0, len(m))
	for _, endpoint := range m {
		out = append(out, endpoint)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Addr < out[j].Addr })
	return out
}

func (r *Registry) nextBackoff(d time.Duration) time.Duration {
	d *= 2
	if d > retryMax {
		return retryMax
	}
	return d
}

func (r *Registry) sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-r.ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func (r *Registry) deliver(callback func([]registry.Endpoint), snap []registry.Endpoint) {
	if callback == nil {
		return
	}
	defer func() {
		if rec := recover(); rec != nil {
			r.log.Error("etcd registry: watcher callback panic", "panic", rec)
		}
	}()
	callback(snap)
}

type registration struct {
	reg   *Registry
	key   string
	val   string
	lease atomic.Int64
}

func (r *Registry) Register(ctx context.Context, name string, endpoint registry.Endpoint) (func(), error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := r.validateName(name); err != nil {
		return nil, err
	}
	if err := r.validateAddr(endpoint.Addr); err != nil {
		return nil, err
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil, registry.ErrClosed
	}
	r.mu.Unlock()

	val, err := json.Marshal(endpoint)
	if err != nil {
		return nil, fmt.Errorf("etcd registry: marshal endpoint: %w", err)
	}
	entry := &registration{reg: r, key: r.kvPrefix(name) + endpoint.Addr, val: string(val)}

	if err := entry.registerOnce(ctx); err != nil {
		return nil, err
	}

	regCtx, cancel := context.WithCancel(context.Background())
	stopCaller := context.AfterFunc(ctx, cancel)
	stopReg := context.AfterFunc(r.ctx, cancel)

	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		cancel()
		stopCaller()
		stopReg()
		entry.revoke()
		return nil, registry.ErrClosed
	}
	r.wg.Add(1)
	r.mu.Unlock()

	go func() {
		defer r.wg.Done()
		defer stopCaller()
		defer stopReg()
		entry.keepLoop(regCtx)
		entry.revoke()
	}()
	return func() { cancel() }, nil
}

func (entry *registration) registerOnce(ctx context.Context) error {
	ttl := int64(math.Ceil(entry.reg.conf.leaseTTL.Seconds()))
	if ttl < 1 {
		ttl = 1
	}
	lease, err := entry.reg.client.Grant(ctx, ttl)
	if err != nil {
		return fmt.Errorf("etcd registry: grant lease: %w", err)
	}
	if _, err := entry.reg.client.Put(ctx, entry.key, entry.val, clientv3.WithLease(lease.ID)); err != nil {
		revokeContext, rcancel := context.WithTimeout(context.Background(), revokeTimeout)
		_, _ = entry.reg.client.Revoke(revokeContext, lease.ID)
		rcancel()
		return fmt.Errorf("etcd registry: put: %w", err)
	}
	entry.lease.Store(int64(lease.ID))
	return nil
}

func (entry *registration) keepLoop(ctx context.Context) {
	backoff := retryBase
	for {
		id := entry.lease.Load()
		if id == 0 {
			if err := entry.registerOnce(ctx); err != nil {
				if ctx.Err() != nil {
					return
				}
				entry.reg.log.Warn("etcd registry: re-register failed", "key", entry.key, "err", err)
				if !entry.reg.sleepCtx(ctx, backoff) {
					return
				}
				backoff = entry.reg.nextBackoff(backoff)
				continue
			}
			backoff = retryBase
			id = entry.lease.Load()
		}

		ch, err := entry.reg.client.KeepAlive(ctx, clientv3.LeaseID(id))
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			entry.reg.log.Warn("etcd registry: keepalive failed", "key", entry.key, "err", err)
			entry.lease.Store(0)
			if !entry.reg.sleepCtx(ctx, backoff) {
				return
			}
			backoff = entry.reg.nextBackoff(backoff)
			continue
		}
	leaseLoop:
		for {
			select {
			case _, ok := <-ch:
				if !ok {
					entry.lease.Store(0)
					break leaseLoop
				}
			case <-ctx.Done():
				return
			}
		}
		if !entry.reg.sleepCtx(ctx, backoff) {
			return
		}
		backoff = entry.reg.nextBackoff(backoff)
	}
}

func (entry *registration) revoke() {
	id := entry.lease.Swap(0)
	if id == 0 {
		return
	}
	revokeContext, cancel := context.WithTimeout(context.Background(), revokeTimeout)
	defer cancel()
	_, _ = entry.reg.client.Revoke(revokeContext, clientv3.LeaseID(id))
}

func (r *Registry) Watch(ctx context.Context, name string, callback func([]registry.Endpoint)) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := r.validateName(name); err != nil {
		return err
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return registry.ErrClosed
	}
	r.wg.Add(1)
	r.mu.Unlock()

	go func() {
		defer r.wg.Done()
		r.watchLoop(ctx, name, callback)
	}()
	return nil
}

func (r *Registry) watchLoop(ctx context.Context, name string, callback func([]registry.Endpoint)) {
	p := r.kvPrefix(name)
	backoff := retryBase
	for ctx.Err() == nil && r.ctx.Err() == nil {
		state, rev, err := r.snapshotAt(ctx, p)
		if err != nil {
			if ctx.Err() != nil || r.ctx.Err() != nil {
				return
			}
			r.log.Warn("etcd registry: initial get failed", "name", name, "err", err)
			if !r.sleepCtx(ctx, backoff) {
				return
			}
			backoff = r.nextBackoff(backoff)
			continue
		}
		backoff = retryBase
		r.deliver(callback, r.snapshotOf(state))

		watchCh := r.client.Watch(ctx, p, clientv3.WithPrefix(), clientv3.WithRev(rev+1))
		r.pump(ctx, p, watchCh, state, callback)
		if ctx.Err() != nil || r.ctx.Err() != nil {
			return
		}
		if !r.sleepCtx(ctx, backoff) {
			return
		}
	}
}

func (r *Registry) pump(ctx context.Context, p string, watchCh clientv3.WatchChan, state map[string]registry.Endpoint, callback func([]registry.Endpoint)) {
	for {
		select {
		case watchResp, ok := <-watchCh:
			if !ok {
				return
			}
			if err := watchResp.Err(); err != nil {
				r.log.Warn("etcd registry: watch interrupted, resync", "prefix", p, "err", err)
				return
			}
			r.apply(p, state, watchResp.Events)
			coalescing := true
			for coalescing {
				select {
				case wresp2, ok2 := <-watchCh:
					if !ok2 {
						return
					}
					if err := wresp2.Err(); err != nil {
						r.log.Warn("etcd registry: watch interrupted, resync", "prefix", p, "err", err)
						return
					}
					r.apply(p, state, wresp2.Events)
				default:
					coalescing = false
				}
			}
			if ctx.Err() != nil || r.ctx.Err() != nil {
				return
			}
			r.deliver(callback, r.snapshotOf(state))
		case <-ctx.Done():
			return
		case <-r.ctx.Done():
			return
		}
	}
}

func (r *Registry) snapshotAt(ctx context.Context, p string) (map[string]registry.Endpoint, int64, error) {
	resp, err := r.client.Get(ctx, p, clientv3.WithPrefix())
	if err != nil {
		return nil, 0, err
	}
	state := make(map[string]registry.Endpoint, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		addr, endpoint, ok := r.parseKV(p, kv.Key, kv.Value)
		if !ok {
			r.log.Warn("etcd registry: bad value dropped", "key", string(kv.Key))
			continue
		}
		state[addr] = endpoint
	}
	return state, resp.Header.Revision, nil
}

func (r *Registry) apply(p string, state map[string]registry.Endpoint, evs []*clientv3.Event) {
	for _, ev := range evs {
		switch ev.Type {
		case clientv3.EventTypePut:
			if addr, endpoint, ok := r.parseKV(p, ev.Kv.Key, ev.Kv.Value); ok {
				state[addr] = endpoint
			}
		case clientv3.EventTypeDelete:
			delete(state, strings.TrimPrefix(string(ev.Kv.Key), p))
		}
	}
}

// Close 终止订阅与续约并关闭本包创建的连接。
func (r *Registry) Close() {
	r.closeOnce.Do(func() {
		r.mu.Lock()
		r.closed = true
		r.mu.Unlock()
		r.cancel()
		r.wg.Wait()
		if r.conf.clientOwned {
			_ = r.client.Close()
		}
	})
}
