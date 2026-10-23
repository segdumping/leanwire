// Package codec
// 消息与编解码（大端）：| len:u32 | id:u32 | body[len] |。
package codec

import (
	"encoding/binary"
	"errors"
	"io"
)

var ErrTooLarge = errors.New("message too large")

const (
	// ReservedIDMin 保留段下界（含）
	ReservedIDMin uint32 = 0xF000
	// ReservedIDMax 保留段上界（含）
	ReservedIDMax uint32 = 0xFFFF
)

// 保留消息号：插件从此处取号，勿在各自包内散落魔数。
const (
	IDHeartbeat uint32 = 0xF001
	IDRPC       uint32 = 0xF002
	IDTUN       uint32 = 0xF005
)

// IsReserved 报告 id 是否在框架保留段
func IsReserved(id uint32) bool { return id >= ReservedIDMin && id <= ReservedIDMax }

type Msg struct {
	ID   uint32
	Body []byte // Send 之后不可修改
}

type Codec interface {
	ReadMsg(r io.Reader) (Msg, error)
	WriteMsg(w io.Writer, m Msg) error
}

func DefaultCodec(maxBody int) Codec {
	if maxBody < 0 {
		maxBody = 0
	}
	return &defaultCodec{maxBody: maxBody}
}

// 帧格式头 = len(u32) + id(u32)，大端
const headerLen = 4 + 4

type defaultCodec struct{ maxBody int }

func (c *defaultCodec) ReadMsg(r io.Reader) (Msg, error) {
	var hb [headerLen]byte
	if _, err := io.ReadFull(r, hb[:]); err != nil {
		return Msg{}, err
	}
	bodyLen := binary.BigEndian.Uint32(hb[0:4])
	if uint64(bodyLen) > uint64(c.maxBody) {
		return Msg{}, ErrTooLarge // 先校验长度再分配，防分配炸弹
	}
	m := Msg{ID: binary.BigEndian.Uint32(hb[4:8])}
	if bodyLen > 0 {
		m.Body = make([]byte, bodyLen)
		if _, err := io.ReadFull(r, m.Body); err != nil {
			return Msg{}, err
		}
	}
	return m, nil
}

func (c *defaultCodec) WriteMsg(w io.Writer, m Msg) error {
	if uint64(len(m.Body)) > uint64(c.maxBody) {
		return ErrTooLarge
	}
	var hb [headerLen]byte
	binary.BigEndian.PutUint32(hb[0:4], uint32(len(m.Body)))
	binary.BigEndian.PutUint32(hb[4:8], m.ID)
	if _, err := w.Write(hb[:]); err != nil {
		return err
	}
	if len(m.Body) > 0 {
		if _, err := w.Write(m.Body); err != nil {
			return err
		}
	}
	return nil
}
