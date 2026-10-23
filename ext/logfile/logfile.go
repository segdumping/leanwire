package logfile

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
)

const (
	defaultMaxSize    = 64 << 20
	defaultMaxBackups = 5
	defaultBuffer     = 4096
)

var ErrClosed = errors.New("logfile: writer closed")

type config struct {
	maxSize    int64
	maxBackups int
	buffer     int
}

type Option func(*config)

// WithMaxSize 设置单文件轮转阈值，<=0 不轮转。
func WithMaxSize(n int64) Option { return func(c *config) { c.maxSize = n } }

// WithMaxBackups 设置保留的历史文件数。
func WithMaxBackups(n int) Option {
	return func(c *config) {
		if n >= 0 {
			c.maxBackups = n
		}
	}
}

// WithBuffer 设置异步队列长度。
func WithBuffer(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.buffer = n
		}
	}
}

type Writer struct {
	path string
	conf config

	f    *os.File
	size int64

	mu        sync.RWMutex
	closed    bool
	ch        chan []byte
	exited    chan struct{}
	closeOnce sync.Once
	dropped   atomic.Uint64
}

func New(path string, opts ...Option) (*Writer, error) {
	conf := config{maxSize: defaultMaxSize, maxBackups: defaultMaxBackups, buffer: defaultBuffer}
	for _, o := range opts {
		o(&conf)
	}
	f, size, err := openAppend(path)
	if err != nil {
		return nil, err
	}
	w := &Writer{
		path:   path,
		conf:   conf,
		f:      f,
		size:   size,
		ch:     make(chan []byte, conf.buffer),
		exited: make(chan struct{}),
	}
	go w.run()
	return w, nil
}

// Write 入队一条日志；队列满丢弃并计数。
func (w *Writer) Write(p []byte) (int, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if w.closed {
		return 0, ErrClosed
	}
	b := append([]byte(nil), p...)
	select {
	case w.ch <- b:
	default:
		w.dropped.Add(1)
	}
	return len(p), nil
}

func (w *Writer) Dropped() uint64 { return w.dropped.Load() }

// Close 排空队列并关闭文件；幂等。
func (w *Writer) Close() error {
	w.closeOnce.Do(func() {
		w.mu.Lock()
		w.closed = true
		close(w.ch)
		w.mu.Unlock()
		<-w.exited
	})
	return nil
}

func (w *Writer) run() {
	defer close(w.exited)
	for b := range w.ch {
		w.write(b)
	}
	if w.f != nil {
		_ = w.f.Close()
	}
}

func (w *Writer) write(b []byte) {
	if w.f == nil {
		w.dropped.Add(1)
		return
	}
	if w.conf.maxSize > 0 && w.size > 0 && w.size+int64(len(b)) > w.conf.maxSize {
		if err := w.rotate(); err != nil {
			w.dropped.Add(1)
			return
		}
	}
	n, err := w.f.Write(b)
	w.size += int64(n)
	if err != nil {
		w.dropped.Add(1)
	}
}

func (w *Writer) rotate() error {
	_ = w.f.Close()
	if w.conf.maxBackups == 0 {
		_ = os.Remove(w.path)
	} else {
		_ = os.Remove(fmt.Sprintf("%s.%d", w.path, w.conf.maxBackups))
		for i := w.conf.maxBackups - 1; i >= 1; i-- {
			_ = os.Rename(fmt.Sprintf("%s.%d", w.path, i), fmt.Sprintf("%s.%d", w.path, i+1))
		}
		if err := os.Rename(w.path, w.path+".1"); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	f, size, err := openAppend(w.path)
	if err != nil {
		w.f = nil
		return err
	}
	w.f, w.size = f, size
	return nil
}

func openAppend(path string) (*os.File, int64, error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, 0, err
	}
	fileInfo, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, 0, err
	}
	return f, fileInfo.Size(), nil
}
