package legacy

// golden 字节锚定帧形，另测往返、前缀校验与上限口径。

import (
	"bytes"
	"testing"

	"github.com/segdumping/leanwire/codec"
)

// golden：| extlen:u8 | len:u32 LE | id:u32 LE | ext | body |。
func TestWriteGolden(t *testing.T) {
	c := New(1024)
	var buffer bytes.Buffer
	m := codec.Msg{ID: 0x9999, Body: WrapExt([]byte("abc"), []byte("xyz"))}
	if err := c.WriteMsg(&buffer, m); err != nil {
		t.Fatal(err)
	}
	want := []byte{3, 3, 0, 0, 0, 0x99, 0x99, 0, 0, 'a', 'b', 'c', 'x', 'y', 'z'}
	if !bytes.Equal(buffer.Bytes(), want) {
		t.Fatalf("got % x\nwant % x", buffer.Bytes(), want)
	}
}

// 往返：非空 ext 与 extlen=0 两种 body 均原样还原。
func TestRoundTrip(t *testing.T) {
	c := New(1024)
	for _, body := range [][]byte{
		WrapExt([]byte("abc"), []byte("xyz")),
		WrapExt(nil, []byte("xy")),
	} {
		var buffer bytes.Buffer
		if err := c.WriteMsg(&buffer, codec.Msg{ID: 0x12345678, Body: body}); err != nil {
			t.Fatal(err)
		}
		got, err := c.ReadMsg(bytes.NewReader(buffer.Bytes()))
		if err != nil || got.ID != 0x12345678 || !bytes.Equal(got.Body, body) {
			t.Fatalf("roundtrip: %+v, %v", got, err)
		}
	}
}

// 缺前缀：空 body、extlen 越过 body 均拒绝。
func TestShortExt(t *testing.T) {
	c := New(1024)
	if err := c.WriteMsg(&bytes.Buffer{}, codec.Msg{ID: 1}); err != ErrShortExt {
		t.Fatalf("empty body: want ErrShortExt, got %v", err)
	}
	if err := c.WriteMsg(&bytes.Buffer{}, codec.Msg{ID: 1, Body: []byte{5}}); err != ErrShortExt {
		t.Fatalf("extlen over body: want ErrShortExt, got %v", err)
	}
	if _, _, err := SplitExt(nil); err != ErrShortExt {
		t.Fatalf("SplitExt(nil): want ErrShortExt, got %v", err)
	}
}

// 超限：读侧先校验后分配。
func TestTooLarge(t *testing.T) {
	c := New(4)
	frame := []byte{1, 8, 0, 0, 0, 1, 0, 0, 0, 'e', 'a', 'b', 'c', 'd', 'f', 'g', 'h'}
	if _, err := c.ReadMsg(bytes.NewReader(frame)); err != codec.ErrTooLarge {
		t.Fatalf("want ErrTooLarge, got %v", err)
	}
}

// 读写同口径：ext 计入 maxBody，同配置两端互不踢。
func TestMaxBodySymmetry(t *testing.T) {
	const maxBody = 8
	c := New(maxBody)
	overBody := WrapExt([]byte("abc"), []byte("pqrstu"))
	if err := c.WriteMsg(&bytes.Buffer{}, codec.Msg{ID: 1, Body: overBody}); err != codec.ErrTooLarge {
		t.Fatalf("ext+payload over maxBody: want ErrTooLarge, got %v", err)
	}
	okBody := WrapExt([]byte("ab"), []byte("pqrstu"))
	var buffer bytes.Buffer
	if err := c.WriteMsg(&buffer, codec.Msg{ID: 7, Body: okBody}); err != nil {
		t.Fatalf("boundary frame rejected: %v", err)
	}
	m, err := c.ReadMsg(bytes.NewReader(buffer.Bytes()))
	if err != nil || m.ID != 7 || !bytes.Equal(m.Body, okBody) {
		t.Fatalf("boundary roundtrip: %+v, %v", m, err)
	}
}

// 负 maxBody 按 0：空 body 可写，有载荷即拒。
func TestNegativeMaxBodyClamped(t *testing.T) {
	c := New(-1)
	var buffer bytes.Buffer
	if err := c.WriteMsg(&buffer, codec.Msg{ID: 1, Body: WrapExt(nil, nil)}); err != nil {
		t.Fatalf("empty body should fit: %v", err)
	}
	if err := c.WriteMsg(&bytes.Buffer{}, codec.Msg{ID: 1, Body: WrapExt(nil, []byte("x"))}); err != codec.ErrTooLarge {
		t.Fatalf("want ErrTooLarge, got %v", err)
	}
}
