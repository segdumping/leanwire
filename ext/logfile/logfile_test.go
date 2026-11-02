package logfile

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func countLines(t *testing.T, path string) int {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return bytes.Count(b, []byte("\n"))
}

// 按大小轮转：只保留 maxBackups 个历史文件，最新一条在当前文件中。
func TestRotateKeepsBackups(t *testing.T) {
	path := filepath.Join(t.TempDir(), "service.log")
	w, err := New(path, WithMaxSize(200), WithMaxBackups(2))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		if _, err := fmt.Fprintf(w, "line-%02d 0123456789\n", i); err != nil { // 20 字节/行
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(path + ".3"); !os.IsNotExist(err) {
		t.Fatalf("backup .3 should not exist (err=%v)", err)
	}
	cur, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(cur, []byte("line-49")) {
		t.Fatalf("current file misses newest line: %q", cur)
	}
	if countLines(t, path+".1") == 0 || countLines(t, path+".2") == 0 {
		t.Fatal("expected two populated backups")
	}
}

// 队列满时丢弃并计数，绝不阻塞写入方；写入文件行数 + 丢弃数 == 写入总数。
func TestDropWhenFullAccounting(t *testing.T) {
	path := filepath.Join(t.TempDir(), "burst.log")
	w, err := New(path, WithMaxSize(0), WithBuffer(1))
	if err != nil {
		t.Fatal(err)
	}
	const total = 1000
	for i := 0; i < total; i++ {
		if _, err := w.Write([]byte("x\n")); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	written := countLines(t, path)
	if uint64(written)+w.Dropped() != total {
		t.Fatalf("written %d + dropped %d != %d", written, w.Dropped(), total)
	}
}

// Close 后写入返回 ErrClosed；Close 幂等。
func TestWriteAfterClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "closed.log")
	w, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	_ = w.Close()
	_ = w.Close()
	if _, err := w.Write([]byte("late\n")); !errors.Is(err, ErrClosed) {
		t.Fatalf("write after close: err = %v, want ErrClosed", err)
	}
}

// Close 与并发 Write 竞争时不丢已接受的条目：每条 Write 返回 nil 的记录
// 要么写入文件、要么计入 Dropped。
func TestCloseRaceNoLostEntries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "race.log")
	w, err := New(path, WithMaxSize(0), WithBuffer(64))
	if err != nil {
		t.Fatal(err)
	}

	const writers = 8
	var (
		wg       sync.WaitGroup
		accepted atomic.Uint64
	)
	stop := make(chan struct{})
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if _, err := w.Write([]byte("x\n")); err == nil {
					accepted.Add(1)
				} else if errors.Is(err, ErrClosed) {
					return
				}
			}
		}()
	}
	time.Sleep(20 * time.Millisecond)
	_ = w.Close() // 与写入者并发
	close(stop)
	wg.Wait()

	written := countLines(t, path)
	if uint64(written)+w.Dropped() != accepted.Load() {
		t.Fatalf("written %d + dropped %d != accepted %d",
			written, w.Dropped(), accepted.Load())
	}
}

// 与 slog 配合：handler 直接写入 Writer，不因丢弃报错。
func TestSlogIntegration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "slog.log")
	w, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewJSONHandler(w, nil))
	log.Info("hello", "uid", 42)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(path)
	if !bytes.Contains(b, []byte(`"msg":"hello"`)) {
		t.Fatalf("slog line missing: %s", b)
	}
}

// rotate 失败后写入不再永久死亡：置空句柄、下次写入先 reopen 重试，LastError 可观测。
func TestRotateFailureSelfHeal(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	w, err := New(path, WithMaxSize(64), WithMaxBackups(1), WithBuffer(8))
	if err != nil {
		t.Fatal(err)
	}
	const msg1 = "first-000000000000000000000000000000\n"
	const msg2 = "second-000000000000000000000000000\n"
	if _, err := fmt.Fprint(w, msg1); err != nil {
		t.Fatal(err)
	}

	// 阻断轮转：.1 是带内容的目录，maxBackups=1 时 remove/rename 均不可移除
	if err := os.Mkdir(path+".1", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path+".1", "placeholder"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := fmt.Fprint(w, msg2); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if w.LastError() != nil && w.Dropped() > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if w.LastError() == nil {
		t.Fatal("LastError must surface the rotate failure")
	}

	// 消除阻断后自愈：reopen + rotate 成功，LastError 清空
	if err := os.RemoveAll(path + ".1"); err != nil {
		t.Fatal(err)
	}
	const msg3 = "third-00000000000000000000000000000\n"
	if _, err := fmt.Fprint(w, msg3); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if w.LastError() == nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if w.LastError() != nil {
		t.Fatalf("LastError after heal = %v", w.LastError())
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	b1, err := os.ReadFile(path + ".1")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(b1, []byte(msg1)) {
		t.Fatalf("rotated backup = %q, want msg1", b1)
	}
	b3, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(b3, []byte(msg3)) {
		t.Fatalf("current file = %q, want msg3", b3)
	}
}
