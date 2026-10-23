package legacy

import (
	"encoding/binary"
	"errors"
	"io"

	"github.com/segdumping/leanwire/codec"
)

const (
	extLenSz = 1
	uint32Sz = 4
)

const hdrSz = extLenSz + 2*uint32Sz

var ErrShortExt = errors.New("legacy: body missing ext length prefix")

type Codec struct {
	maxBody int
}

var _ codec.Codec = (*Codec)(nil)

func New(maxBody int) codec.Codec {
	if maxBody < 0 {
		maxBody = 0
	}
	return &Codec{maxBody: maxBody}
}

// WrapExt 组装 body 的 ext 前缀。
func WrapExt(ext, payload []byte) []byte {
	if len(ext) > 0xFF {
		panic("legacy: ext exceeds 255 bytes")
	}
	b := make([]byte, 0, extLenSz+len(ext)+len(payload))
	b = append(b, byte(len(ext)))
	b = append(b, ext...)
	return append(b, payload...)
}

func SplitExt(body []byte) (ext, payload []byte, err error) {
	if len(body) == 0 {
		return nil, nil, ErrShortExt
	}
	extLen := int(body[0])
	if extLenSz+extLen > len(body) {
		return nil, nil, ErrShortExt
	}
	return body[extLenSz : extLenSz+extLen], body[extLenSz+extLen:], nil
}

func (c *Codec) ReadMsg(r io.Reader) (codec.Msg, error) {
	hs := make([]byte, extLenSz+uint32Sz)
	if _, err := io.ReadFull(r, hs); err != nil {
		return codec.Msg{}, err
	}
	extLen := uint32(hs[0])
	msgLen := binary.LittleEndian.Uint32(hs[extLenSz:])
	if uint64(extLen)+uint64(msgLen) > uint64(c.maxBody) {
		return codec.Msg{}, codec.ErrTooLarge
	}
	rest := make([]byte, uint32Sz+int(extLen)+int(msgLen))
	if _, err := io.ReadFull(r, rest); err != nil {
		return codec.Msg{}, err
	}
	m := codec.Msg{ID: binary.LittleEndian.Uint32(rest[0:uint32Sz])}
	m.Body = append(m.Body, hs[0])
	m.Body = append(m.Body, rest[uint32Sz:uint32Sz+int(extLen)]...)
	m.Body = append(m.Body, rest[uint32Sz+int(extLen):]...)
	return m, nil
}

func (c *Codec) WriteMsg(w io.Writer, m codec.Msg) error {
	ext, payload, err := SplitExt(m.Body)
	if err != nil {
		return err
	}
	if len(ext)+len(payload) > c.maxBody {
		return codec.ErrTooLarge
	}
	buffer := make([]byte, hdrSz+len(ext)+len(payload))
	buffer[0] = byte(len(ext))
	binary.LittleEndian.PutUint32(buffer[extLenSz:], uint32(len(payload)))
	binary.LittleEndian.PutUint32(buffer[extLenSz+uint32Sz:], m.ID)
	copy(buffer[hdrSz:], ext)
	copy(buffer[hdrSz+len(ext):], payload)
	_, err = w.Write(buffer)
	return err
}
