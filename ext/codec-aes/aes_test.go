package gateaes

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/segdumping/leanwire/codec"
)

// 测试密钥材料：key 32 字节、IV 16 字节。
const (
	testKey = "0123456789abcdef0123456789abcdef"
	testIV  = "fedcba9876543210"
)

// testCodec 带测试 key/IV 构造；其余选项原样透传。
func testCodec(opts ...Option) codec.Codec {
	return NewCodec(append([]Option{WithKey(testKey), WithIV(testIV)}, opts...)...)
}

// secureCodec 带测试 key 构造 SecureCodec；其余选项原样透传。
func secureCodec(opts ...Option) *SecureCodec {
	return NewSecureCodec(append([]Option{WithKey(testKey)}, opts...)...).(*SecureCodec)
}

// TestRoundtripAccumulated 单连接回环：3 条消息共享同一条 CTR 流。
func TestRoundtripAccumulated(t *testing.T) {
	msgs := []codec.Msg{
		{ID: 7, Body: []byte("hello")},
		{ID: 0x20260224, Body: bytes.Repeat([]byte{0xA5}, 4097)},
		{ID: 255, Body: nil}, // 空体边界
	}

	var out bytes.Buffer
	bw := bufio.NewWriter(&out)
	enc := testCodec()
	for i, m := range msgs {
		if err := enc.WriteMsg(bw, m); err != nil {
			t.Fatalf("write msg %d: %v", i, err)
		}
	}
	if err := bw.Flush(); err != nil {
		t.Fatal(err)
	}

	dec := testCodec()
	br := bufio.NewReader(bytes.NewReader(out.Bytes()))
	for i, want := range msgs {
		got, err := dec.ReadMsg(br)
		if err != nil {
			t.Fatalf("read msg %d: %v", i, err)
		}
		if got.ID != want.ID || !bytes.Equal(got.Body, want.Body) {
			t.Fatalf("msg %d mismatch: got id=%d len=%d, want id=%d len=%d", i, got.ID, len(got.Body), want.ID, len(want.Body))
		}
	}
	if _, err := dec.ReadMsg(br); !errors.Is(err, io.EOF) {
		t.Fatalf("期望流尽 EOF，得到 %v", err)
	}
}

// TestPerSessionStreamsIndependent 两连接各自独立流，互不继承推进位置。
func TestPerSessionStreamsIndependent(t *testing.T) {
	c := testCodec().(*Codec)
	m := codec.Msg{ID: 99, Body: []byte("same-plaintext")}

	write := func(n int) []byte {
		var sink bytes.Buffer
		bw := bufio.NewWriter(&sink)
		for i := 0; i < n; i++ {
			if err := c.WriteMsg(bw, m); err != nil {
				t.Fatal(err)
			}
		}
		if err := bw.Flush(); err != nil {
			t.Fatal(err)
		}
		return sink.Bytes()
	}

	a := write(3)
	b := write(1)

	fl := 8 + len(m.Body) // 每帧：msglen(4) + id(4) + msg
	if len(a) != 3*fl || len(b) != fl {
		t.Fatalf("帧长异常：a=%d b=%d", len(a), len(b))
	}
	if !bytes.Equal(a[:fl], b) {
		t.Fatal("连接 B 首帧密文应与连接 A 首帧一致（各自流位置 0）")
	}
	if bytes.Equal(a[fl:2*fl], a[:fl]) {
		t.Fatal("连接 A 第 2 帧应推进流（与首帧不同密文）")
	}
	if bytes.Equal(b, a[fl:2*fl]) {
		t.Fatal("连接 B 首帧不应继承连接 A 的流位置")
	}

	dec := testCodec()
	br := bufio.NewReader(bytes.NewReader(b))
	got, err := dec.ReadMsg(br)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != m.ID || !bytes.Equal(got.Body, m.Body) {
		t.Fatalf("B 密文解码不符：id=%d", got.ID)
	}
}

// TestWeakSweepReapsDeadSessions 死连接条目被弱键收割，存活流不受影响。
func TestWeakSweepReapsDeadSessions(t *testing.T) {
	c := testCodec().(*Codec)
	m := codec.Msg{ID: 1, Body: []byte("x")}

	// 存活连接：writer 持续被引用，弱键不失效
	var sink bytes.Buffer
	live := bufio.NewWriter(&sink)
	if err := c.WriteMsg(live, m); err != nil {
		t.Fatal(err)
	}
	registerEphemeral(c)
	registerEphemeral(c)

	waitDead := func() bool {
		for i := 0; i < 5; i++ {
			runtime.GC()
			runtime.GC()
			time.Sleep(10 * time.Millisecond)
			alive, dead := 0, 0
			for k := range c.wr {
				if k.Value() == nil {
					dead++
				} else {
					alive++
				}
			}
			t.Logf("round %d: alive=%d dead=%d", i, alive, dead)
			if dead == 2 && alive == 1 {
				return true
			}
		}
		return false
	}
	if !waitDead() {
		t.Skip("GC 未在有限轮次内确认死键（环境时序相关），跳过收割断言")
	}

	c.sweepLocked()
	if len(c.wr) != 1 {
		t.Fatalf("收割后应剩 1 条存活流，实际 %d 条", len(c.wr))
	}

	// 存活流收割后须继续累积，不得重开。
	if err := c.WriteMsg(live, m); err != nil {
		t.Fatal(err)
	}
	if err := c.WriteMsg(live, m); err != nil {
		t.Fatal(err)
	}
	if err := live.Flush(); err != nil {
		t.Fatal(err)
	}
	dec := testCodec()
	br := bufio.NewReader(bytes.NewReader(sink.Bytes()))
	for i := 0; i < 3; i++ {
		got, err := dec.ReadMsg(br)
		if err != nil {
			t.Fatalf("post-sweep msg %d: %v", i, err)
		}
		if got.ID != m.ID {
			t.Fatalf("post-sweep msg %d id=%d", i, got.ID)
		}
	}
}

func registerEphemeral(c *Codec) {
	bw := bufio.NewWriter(io.Discard)
	_, _ = c.writeStream(bw) // 返回后 bw 不可达，弱键待收割
}

// TestBoundsAndShortFrame 超限与截断帧错误行为。
func TestBoundsAndShortFrame(t *testing.T) {
	c := testCodec()

	if err := c.WriteMsg(bufio.NewWriter(io.Discard), codec.Msg{ID: 1, Body: bytes.Repeat([]byte{0}, 102401)}); !errors.Is(err, codec.ErrTooLarge) {
		t.Fatalf("写超限应 ErrTooLarge，得到 %v", err)
	}

	// 伪造超长 msglen，只给头
	head := make([]byte, 4)
	binary.LittleEndian.PutUint32(head, 102401)
	if _, err := c.ReadMsg(bytes.NewReader(head)); !errors.Is(err, codec.ErrTooLarge) {
		t.Fatalf("读超限应 ErrTooLarge，得到 %v", err)
	}

	if _, err := c.ReadMsg(bytes.NewReader([]byte{1, 2})); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("头截断应 ErrUnexpectedEOF，得到 %v", err)
	}
	if _, err := c.ReadMsg(bytes.NewReader(nil)); !errors.Is(err, io.EOF) {
		t.Fatalf("空流应 EOF，得到 %v", err)
	}

	// 体截断：裁掉帧尾两字节
	var full bytes.Buffer
	bw := bufio.NewWriter(&full)
	if err := c.WriteMsg(bw, codec.Msg{ID: 5, Body: []byte("abcdef")}); err != nil {
		t.Fatal(err)
	}
	if err := bw.Flush(); err != nil {
		t.Fatal(err)
	}
	trimmed := full.Bytes()[:full.Len()-2]
	if _, err := c.ReadMsg(bufio.NewReader(bytes.NewReader(trimmed))); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("体截断应 ErrUnexpectedEOF，得到 %v", err)
	}

	// 自定义更小上限
	c2 := testCodec(WithMaxBody(16))
	if err := c2.WriteMsg(bufio.NewWriter(io.Discard), codec.Msg{ID: 1, Body: bytes.Repeat([]byte{0}, 17)}); !errors.Is(err, codec.ErrTooLarge) {
		t.Fatalf("WithMaxBody 上限未生效，得到 %v", err)
	}
}

// TestPlainGoldenWithoutCrypto 关加密帧与裸帧 | len:u32 LE | id:u32 LE | body | 逐位一致且可互读。
func TestPlainGoldenWithoutCrypto(t *testing.T) {
	msgs := []codec.Msg{
		{ID: 7, Body: []byte("abc")},
		{ID: 256, Body: []byte{}},
	}
	want := []byte{
		3, 0, 0, 0, 7, 0, 0, 0, 'a', 'b', 'c', // msg1
		0, 0, 0, 0, 0, 1, 0, 0, // msg2（空体）
	}

	var ours bytes.Buffer
	obw := bufio.NewWriter(&ours)
	plain := NewCodec(WithoutCrypto())
	for _, m := range msgs {
		if err := plain.WriteMsg(obw, m); err != nil {
			t.Fatal(err)
		}
	}
	if err := obw.Flush(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(ours.Bytes(), want) {
		t.Fatalf("got % x\nwant % x", ours.Bytes(), want)
	}

	br := bufio.NewReader(bytes.NewReader(want))
	for i, want := range msgs {
		got, err := plain.ReadMsg(br)
		if err != nil {
			t.Fatalf("read msg %d: %v", i, err)
		}
		if got.ID != want.ID || !bytes.Equal(got.Body, want.Body) {
			t.Fatalf("read msg %d mismatch", i)
		}
	}
}

// TestOptionsCustomKeyIV 自定义 key/IV 密文相异 + 回环 + 构造期缺失/非法 panic。
func TestOptionsCustomKeyIV(t *testing.T) {
	custom := NewCodec(WithKey(testKey), WithIV(testIV))
	other := NewCodec(WithKey("ffffffffffffffffffffffffffffffff"), WithIV(testIV))
	m := codec.Msg{ID: 0x89ABCDEF, Body: []byte("custom key material")}

	write := func(c codec.Codec) []byte {
		var sink bytes.Buffer
		bw := bufio.NewWriter(&sink)
		if err := c.WriteMsg(bw, m); err != nil {
			t.Fatal(err)
		}
		if err := bw.Flush(); err != nil {
			t.Fatal(err)
		}
		return sink.Bytes()
	}
	if bytes.Equal(write(custom), write(other)) {
		t.Fatal("不同 key 的密文不应一致")
	}

	br := bufio.NewReader(bytes.NewReader(write(custom)))
	got, err := custom.ReadMsg(br)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != m.ID || !bytes.Equal(got.Body, m.Body) {
		t.Fatalf("自定义键回环不符：%q", got.Body)
	}

	expectPanic(t, "missing key/iv", func() { NewCodec() })
	expectPanic(t, "missing iv", func() { NewCodec(WithKey(testKey)) })
	expectPanic(t, "bad iv", func() { NewCodec(WithKey(testKey), WithIV("short")) })
	expectPanic(t, "bad key", func() { NewCodec(WithKey("ceci n'est pas une cle"), WithIV(testIV)) })
}

func expectPanic(t *testing.T, name string, f func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatalf("%s：期望构造期 panic", name)
		}
	}()
	f()
}

// TestConcurrentSessions 多连接并发经同一实例收发。
func TestConcurrentSessions(t *testing.T) {
	c := testCodec()
	const sessions = 8
	const per = 50

	sinks := make([]*bytes.Buffer, sessions)
	done := make(chan error, sessions)
	var wg sync.WaitGroup
	for s := 0; s < sessions; s++ {
		sinks[s] = new(bytes.Buffer)
		wg.Add(1)
		go func(s int) {
			defer wg.Done()
			bw := bufio.NewWriter(sinks[s])
			for i := 0; i < per; i++ {
				m := codec.Msg{ID: uint32(1000 + s), Body: fmt.Appendf(nil, "s%d-m%d", s, i)}
				if err := c.WriteMsg(bw, m); err != nil {
					done <- err
					return
				}
			}
			if err := bw.Flush(); err != nil {
				done <- err
				return
			}
			done <- nil
		}(s)
	}
	wg.Wait()
	close(done)

	for err := range done {
		if err != nil {
			t.Fatal(err)
		}
	}
	for s := 0; s < sessions; s++ {
		dec := testCodec()
		br := bufio.NewReader(bytes.NewReader(sinks[s].Bytes()))
		for i := 0; i < per; i++ {
			got, err := dec.ReadMsg(br)
			if err != nil {
				t.Fatalf("session %d msg %d: %v", s, i, err)
			}
			want := fmt.Appendf(nil, "s%d-m%d", s, i)
			if got.ID != uint32(1000+s) || !bytes.Equal(got.Body, want) {
				t.Fatalf("session %d msg %d 内容不符", s, i)
			}
		}
	}
}

// ── SecureCodec（每连接 nonce）──────────────────────────────────────────

// TestSecureRoundtrip 单连接回环：首帧前只出现一次 nonce 前缀，同一条流
// 累计解密。
func TestSecureRoundtrip(t *testing.T) {
	msgs := []codec.Msg{
		{ID: 7, Body: []byte("hello")},
		{ID: 0x20260224, Body: bytes.Repeat([]byte{0xA5}, 4097)},
		{ID: 255, Body: nil}, // 空体边界
	}

	var out bytes.Buffer
	bw := bufio.NewWriter(&out)
	enc := secureCodec()
	for i, m := range msgs {
		if err := enc.WriteMsg(bw, m); err != nil {
			t.Fatalf("write msg %d: %v", i, err)
		}
	}
	if err := bw.Flush(); err != nil {
		t.Fatal(err)
	}
	raw := out.Bytes()

	// nonce 前缀恰好一次：总长 = 16 + ∑(4+4+body)。
	var want int
	for _, m := range msgs {
		want += 8 + len(m.Body)
	}
	if len(raw) != nonceSize+want {
		t.Fatalf("wire len = %d, want %d (16B nonce + frames)", len(raw), nonceSize+want)
	}

	dec := secureCodec()
	br := bufio.NewReader(bytes.NewReader(raw))
	for i, w := range msgs {
		got, err := dec.ReadMsg(br)
		if err != nil {
			t.Fatalf("read msg %d: %v", i, err)
		}
		if got.ID != w.ID || !bytes.Equal(got.Body, w.Body) {
			t.Fatalf("msg %d mismatch: got id=%d len=%d, want id=%d len=%d", i, got.ID, len(got.Body), w.ID, len(w.Body))
		}
	}
	if _, err := dec.ReadMsg(br); !errors.Is(err, io.EOF) {
		t.Fatalf("期望流尽 EOF，得到 %v", err)
	}
}

// TestSecurePerConnNoncesDistinct 关键安全断言：同一明文在不同连接上密文
// 不同（每连接每方向随机 nonce，密钥流不复用）。
func TestSecurePerConnNoncesDistinct(t *testing.T) {
	c := secureCodec()
	m := codec.Msg{ID: 99, Body: []byte("same-plaintext")}

	a := doSecureFrame(t, c, m) // 连接 1
	b := doSecureFrame(t, c, m) // 连接 2

	// nonce 前缀（前 16 字节）必须不同
	if bytes.Equal(a[:nonceSize], b[:nonceSize]) {
		t.Fatal("两连接 nonce 相同：CSPRNG 未生效")
	}
	// 首帧整体（nonce + 密文）必须不同：同明文同 key 经不同密钥流 ⇒ 密文不同
	if bytes.Equal(a[:nonceSize+4+4+len(m.Body)], b[:nonceSize+4+4+len(m.Body)]) {
		t.Fatal("两连接同明文密文相同：密钥流跨连接复用")
	}
}

// doSecureFrame 写单帧并返回密文（含 nonce 前缀）。
func doSecureFrame(t *testing.T, c *SecureCodec, m codec.Msg) []byte {
	t.Helper()
	var sink bytes.Buffer
	bw := bufio.NewWriter(&sink)
	if err := c.WriteMsg(bw, m); err != nil {
		t.Fatal(err)
	}
	if err := bw.Flush(); err != nil {
		t.Fatal(err)
	}
	return sink.Bytes()
}

// TestSecureWithoutCrypto 明文模式：帧格式同（nonce 前缀 + 明文帧）。
func TestSecureWithoutCrypto(t *testing.T) {
	enc := secureCodec(WithoutCrypto())
	m := codec.Msg{ID: 3, Body: []byte("plain")}

	var out bytes.Buffer
	bw := bufio.NewWriter(&out)
	if err := enc.WriteMsg(bw, m); err != nil {
		t.Fatal(err)
	}
	if err := bw.Flush(); err != nil {
		t.Fatal(err)
	}

	dec := secureCodec(WithoutCrypto())
	got, err := dec.ReadMsg(bufio.NewReader(bytes.NewReader(out.Bytes())))
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != m.ID || !bytes.Equal(got.Body, m.Body) {
		t.Fatalf("got id=%d body=%q", got.ID, got.Body)
	}
}

// TestSecureRejectsIVOption SecureCodec 拒绝 WithIV/WithIVs（IV 由每连接
// nonce 派生，配置期固定 IV 毁掉唯一性）。
func TestSecureRejectsIVOption(t *testing.T) {
	expectPanic(t, "SecureCodec + WithIV", func() {
		NewSecureCodec(WithKey(testKey), WithIV(testIV))
	})
	expectPanic(t, "SecureCodec + WithIVs", func() {
		NewSecureCodec(WithKey(testKey), WithIVs(testIV, testIV))
	})
	expectPanic(t, "SecureCodec 无 key", func() { NewSecureCodec() })
}

// TestSecureWireNoncesIndependent 交叉解密：连接 A 的写流与连接 B 的读流
// 共同验证 nonce 经首帧前缀交付后，读侧以对端 nonce 起流可正确解密——
// 即模拟两端真实互联的全路径。
func TestSecureWireNoncesIndependent(t *testing.T) {
	// 端 A：写侧实例（模拟对端 peer）
	aWrite := secureCodec()
	// 端 B：读侧实例（本端），必须是不同实例（真实部署两端各持一份新实例，
	// key 相同）
	bRead := secureCodec()

	m := codec.Msg{ID: 42, Body: []byte("cross-instance roundtrip")}
	var wire bytes.Buffer
	bw := bufio.NewWriter(&wire)
	if err := aWrite.WriteMsg(bw, m); err != nil {
		t.Fatal(err)
	}
	if err := bw.Flush(); err != nil {
		t.Fatal(err)
	}

	got, err := bRead.ReadMsg(bufio.NewReader(bytes.NewReader(wire.Bytes())))
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != m.ID || !bytes.Equal(got.Body, m.Body) {
		t.Fatalf("got id=%d body=%q", got.ID, got.Body)
	}
}
