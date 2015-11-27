package DataType

import (
	"unicode"
	"unsafe"
)

const (
	HeadLen = uint64(unsafe.Sizeof(uint64(0)))
	MsgLen  = uint32(unsafe.Sizeof(MessageHead{}))
	RecLen  = uint32(unsafe.Sizeof(RecordHead{}))
)

type MessageHead struct {
	Msglen   uint64
	Sender   uint64
	Recver   uint64
	Copyer   uint64
	SendTime uint64
	OutTime  uint64
}

type RecordHead struct {
	HashCode uint64
	RecType  uint32
	Length   uint32
}

func MakeID(name string) uint64 {
	var id uint64
	for i, c := range name {
		if i < 8 && unicode.IsLetter(c) {
			id = id*256 + uint64(c)
		}
	}
	return id
}

func Roll(in float64) float64 {
	if in > 0 {
		return float64(int64((in+0.000005)*10000)) / 10000
	}

	if in < 0 {
		return float64(int64((in-0.000005)*10000)) / 10000
	}

	return 0
}

func Equ(a, b float64) bool {
	return int64(Roll(a)*1000) == int64(Roll(b)*1000)
}
