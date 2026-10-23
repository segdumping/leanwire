// Package gateaes 提供客户端连接编解码器；legacy 用全局 IV，SecureCodec 用每连接 nonce。
// 两者均无消息认证，敏感连接请叠加 TLS。
package gateaes

import (
	"bufio"
	"crypto/aes"
	"crypto/cipher"
	crand "crypto/rand"
	"encoding/binary"
	"io"
	"sync"
	"weak"

	"github.com/segdumping/leanwire/codec"
)

const defaultMaxBody = 102400

const sweepEvery = 64

type config struct {
	key      []byte
	readIV   []byte
	writeIV  []byte
	noCrypto bool
	maxBody  int
}

type Option func(*config)

// WithKey 设置 AES key。
func WithKey(key string) Option {
	return func(conf *config) {
		if key != "" {
			conf.key = []byte(key)
		}
	}
}

// WithIV 设置读写共用的 AES IV（legacy 兼容）。
func WithIV(iv string) Option {
	return func(conf *config) {
		if iv != "" {
			conf.readIV = []byte(iv)
			conf.writeIV = []byte(iv)
		}
	}
}

// WithIVs 按方向分别配置 AES IV。
func WithIVs(readIV, writeIV string) Option {
	return func(conf *config) {
		if readIV != "" {
			conf.readIV = []byte(readIV)
		}
		if writeIV != "" {
			conf.writeIV = []byte(writeIV)
		}
	}
}

// WithMaxBody 配置 body 上限。
func WithMaxBody(n int) Option {
	return func(conf *config) {
		if n > 0 {
			conf.maxBody = n
		}
	}
}

// WithoutCrypto 关闭加密。
func WithoutCrypto() Option {
	return func(conf *config) { conf.noCrypto = true }
}

var _ codec.Codec = (*Codec)(nil)

type Codec struct {
	conf config

	block   cipher.Block
	readIV  []byte
	writeIV []byte

	mu    sync.Mutex
	wr    map[weak.Pointer[bufio.Writer]]cipher.Stream
	rd    map[weak.Pointer[bufio.Reader]]cipher.Stream
	wrAny map[io.Writer]cipher.Stream
	rdAny map[io.Reader]cipher.Stream
	fresh int
}

func NewCodec(opts ...Option) codec.Codec {
	conf := config{maxBody: defaultMaxBody}
	for _, o := range opts {
		o(&conf)
	}
	c := &Codec{
		conf: conf,
		wr:   make(map[weak.Pointer[bufio.Writer]]cipher.Stream),
		rd:   make(map[weak.Pointer[bufio.Reader]]cipher.Stream),
	}
	if conf.noCrypto {
		return c
	}
	if len(conf.key) == 0 || len(conf.readIV) == 0 || len(conf.writeIV) == 0 {
		panic("gateaes: key and iv required")
	}
	block, err := aes.NewCipher(conf.key)
	if err != nil {
		panic("gateaes: bad key: " + err.Error())
	}
	if len(conf.readIV) != block.BlockSize() || len(conf.writeIV) != block.BlockSize() {
		panic("gateaes: bad iv size")
	}
	c.block = block
	c.readIV = conf.readIV
	c.writeIV = conf.writeIV
	c.wrAny = make(map[io.Writer]cipher.Stream)
	c.rdAny = make(map[io.Reader]cipher.Stream)
	return c
}

func (c *Codec) ReadMsg(r io.Reader) (codec.Msg, error) {
	header := make([]byte, 4)
	if _, err := io.ReadFull(r, header); err != nil {
		return codec.Msg{}, err
	}
	msgLen := binary.LittleEndian.Uint32(header)
	if msgLen > uint32(c.conf.maxBody) {
		return codec.Msg{}, codec.ErrTooLarge
	}
	rest := make([]byte, 4+int(msgLen)) // id(4) + msg
	if _, err := io.ReadFull(r, rest); err != nil {
		return codec.Msg{}, err
	}
	if c.block != nil {
		stream, err := c.readStream(r)
		if err != nil {
			return codec.Msg{}, err
		}
		stream.XORKeyStream(rest, rest)
	}
	m := codec.Msg{ID: binary.LittleEndian.Uint32(rest[0:4])}
	m.Body = rest[4:]
	return m, nil
}

func (c *Codec) WriteMsg(w io.Writer, m codec.Msg) error {
	if len(m.Body) > c.conf.maxBody {
		return codec.ErrTooLarge
	}
	buffer := make([]byte, 8+len(m.Body))
	binary.LittleEndian.PutUint32(buffer[0:4], uint32(len(m.Body)))
	binary.LittleEndian.PutUint32(buffer[4:8], m.ID)
	copy(buffer[8:], m.Body)
	if c.block != nil {
		stream, err := c.writeStream(w)
		if err != nil {
			return err
		}
		stream.XORKeyStream(buffer[4:], buffer[4:])
	}
	_, err := w.Write(buffer)
	return err
}

func (c *Codec) writeStream(w io.Writer) (cipher.Stream, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	bw, ok := w.(*bufio.Writer)
	if ok {
		wp := weak.Make(bw)
		if stream, ok2 := c.wr[wp]; ok2 {
			return stream, nil
		}
		stream := c.newCTR(true)
		c.wr[wp] = stream
		c.markFreshAndSweep()
		return stream, nil
	}
	if stream, ok := c.wrAny[w]; ok {
		return stream, nil
	}
	stream := c.newCTR(true)
	c.wrAny[w] = stream
	return stream, nil
}

func (c *Codec) readStream(r io.Reader) (cipher.Stream, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if br, ok := r.(*bufio.Reader); ok {
		wp := weak.Make(br)
		if stream, ok2 := c.rd[wp]; ok2 {
			return stream, nil
		}
		stream := c.newCTR(false)
		c.rd[wp] = stream
		c.markFreshAndSweep()
		return stream, nil
	}
	if stream, ok := c.rdAny[r]; ok {
		return stream, nil
	}
	stream := c.newCTR(false)
	c.rdAny[r] = stream
	return stream, nil
}

func (c *Codec) newCTR(write bool) cipher.Stream {
	iv := c.readIV
	if write {
		iv = c.writeIV
	}
	return cipher.NewCTR(c.block, iv)
}

func (c *Codec) markFreshAndSweep() {
	c.fresh++
	if c.fresh < sweepEvery {
		return
	}
	c.fresh = 0
	c.sweepLocked()
}

func (c *Codec) sweepLocked() {
	for k := range c.wr {
		if k.Value() == nil {
			delete(c.wr, k)
		}
	}
	for k := range c.rd {
		if k.Value() == nil {
			delete(c.rd, k)
		}
	}
}

const nonceSize = 16

type secureStream struct {
	nonce        []byte
	cipherStream cipher.Stream
	first        bool
}

var _ codec.Codec = (*SecureCodec)(nil)

type SecureCodec struct {
	conf  config
	block cipher.Block

	mu    sync.Mutex
	wr    map[weak.Pointer[bufio.Writer]]*secureStream
	rd    map[weak.Pointer[bufio.Reader]]*secureStream
	wrAny map[io.Writer]*secureStream
	rdAny map[io.Reader]*secureStream
	fresh int
}

// NewSecureCodec 返回每连接 nonce 的编解码器。
func NewSecureCodec(opts ...Option) codec.Codec {
	conf := config{maxBody: defaultMaxBody}
	for _, o := range opts {
		o(&conf)
	}
	if conf.readIV != nil || conf.writeIV != nil {
		panic("gateaes: SecureCodec derives IV per connection; WithIV/WithIVs are invalid here")
	}
	c := &SecureCodec{
		conf:  conf,
		wr:    make(map[weak.Pointer[bufio.Writer]]*secureStream),
		rd:    make(map[weak.Pointer[bufio.Reader]]*secureStream),
		wrAny: make(map[io.Writer]*secureStream),
		rdAny: make(map[io.Reader]*secureStream),
	}
	if conf.noCrypto {
		return c
	}
	if len(conf.key) == 0 {
		panic("gateaes: key required")
	}
	block, err := aes.NewCipher(conf.key)
	if err != nil {
		panic("gateaes: bad key: " + err.Error())
	}
	c.block = block
	return c
}

func (c *SecureCodec) ReadMsg(r io.Reader) (codec.Msg, error) {
	stream, err := c.readStream(r)
	if err != nil {
		return codec.Msg{}, err
	}
	return c.readFrame(r, stream)
}

func (c *SecureCodec) readStream(r io.Reader) (*secureStream, error) {
	c.mu.Lock()
	if br, ok := r.(*bufio.Reader); ok {
		wp := weak.Make(br)
		if stream, ok2 := c.rd[wp]; ok2 {
			c.mu.Unlock()
			return stream, nil
		}
	} else if stream, ok := c.rdAny[r]; ok {
		c.mu.Unlock()
		return stream, nil
	}
	c.mu.Unlock()

	stream, err := c.newReadStream(r)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if br, ok := r.(*bufio.Reader); ok {
		wp := weak.Make(br)
		if cur, ok2 := c.rd[wp]; ok2 {
			return cur, nil
		}
		c.rd[wp] = stream
		c.markFreshAndSweep()
	} else {
		if cur, ok := c.rdAny[r]; ok {
			return cur, nil
		}
		c.rdAny[r] = stream
	}
	return stream, nil
}

func (c *SecureCodec) newReadStream(r io.Reader) (*secureStream, error) {
	stream := &secureStream{}
	nonce := make([]byte, nonceSize)
	if _, err := io.ReadFull(r, nonce); err != nil {
		return nil, err
	}
	stream.nonce = nonce
	if c.block != nil {
		stream.cipherStream = cipher.NewCTR(c.block, nonce)
	}
	return stream, nil
}

func (c *SecureCodec) readFrame(r io.Reader, stream *secureStream) (codec.Msg, error) {
	header := make([]byte, 4)
	if _, err := io.ReadFull(r, header); err != nil {
		return codec.Msg{}, err
	}
	msgLen := binary.LittleEndian.Uint32(header)
	if msgLen > uint32(c.conf.maxBody) {
		return codec.Msg{}, codec.ErrTooLarge
	}
	rest := make([]byte, 4+int(msgLen)) // id(4) + msg
	if _, err := io.ReadFull(r, rest); err != nil {
		return codec.Msg{}, err
	}
	if stream.cipherStream != nil {
		stream.cipherStream.XORKeyStream(rest, rest)
	}
	m := codec.Msg{ID: binary.LittleEndian.Uint32(rest[0:4])}
	m.Body = rest[4:]
	return m, nil
}

func (c *SecureCodec) WriteMsg(w io.Writer, m codec.Msg) error {
	if len(m.Body) > c.conf.maxBody {
		return codec.ErrTooLarge
	}
	stream, first, err := c.writeStream(w)
	if err != nil {
		return err
	}
	if first {
		if _, err := w.Write(stream.nonce); err != nil {
			return err
		}
	}
	buffer := make([]byte, 8+len(m.Body))
	binary.LittleEndian.PutUint32(buffer[0:4], uint32(len(m.Body)))
	binary.LittleEndian.PutUint32(buffer[4:8], m.ID)
	copy(buffer[8:], m.Body)
	if stream.cipherStream != nil {
		stream.cipherStream.XORKeyStream(buffer[4:], buffer[4:])
	}
	_, err = w.Write(buffer)
	return err
}

func (c *SecureCodec) writeStream(w io.Writer) (*secureStream, bool, error) {
	c.mu.Lock()
	if bw, ok := w.(*bufio.Writer); ok {
		wp := weak.Make(bw)
		if stream, ok2 := c.wr[wp]; ok2 {
			c.mu.Unlock()
			return stream, false, nil
		}
	} else if stream, ok := c.wrAny[w]; ok {
		c.mu.Unlock()
		return stream, false, nil
	}
	c.mu.Unlock()

	stream := &secureStream{nonce: make([]byte, nonceSize), first: true}
	if _, err := crand.Read(stream.nonce); err != nil {
		return nil, false, err
	}
	if c.block != nil {
		stream.cipherStream = cipher.NewCTR(c.block, stream.nonce)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if bw, ok := w.(*bufio.Writer); ok {
		wp := weak.Make(bw)
		if cur, ok2 := c.wr[wp]; ok2 {
			return cur, false, nil
		}
		c.wr[wp] = stream
		c.markFreshAndSweep()
	} else {
		if cur, ok := c.wrAny[w]; ok {
			return cur, false, nil
		}
		c.wrAny[w] = stream
	}
	return stream, true, nil
}

func (c *SecureCodec) markFreshAndSweep() {
	c.fresh++
	if c.fresh < sweepEvery {
		return
	}
	c.fresh = 0
	c.sweepSecureLocked()
}

func (c *SecureCodec) sweepSecureLocked() {
	for k := range c.wr {
		if k.Value() == nil {
			delete(c.wr, k)
		}
	}
	for k := range c.rd {
		if k.Value() == nil {
			delete(c.rd, k)
		}
	}
}
