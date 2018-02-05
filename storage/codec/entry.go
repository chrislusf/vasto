package codec

import (
	"encoding/binary"
	"time"
)

type OpAndDataType byte

type Entry struct {
	PartitionHash uint64
	UpdatedAtNs   uint64
	TtlSecond     uint32
	OpAndDataType OpAndDataType
	Value         []byte
}

func (e *Entry) ToBytes() []byte {
	b := make([]byte, len(e.Value)+21)

	binary.LittleEndian.PutUint64(b, e.PartitionHash)
	binary.LittleEndian.PutUint64(b[8:], e.UpdatedAtNs)
	binary.LittleEndian.PutUint32(b[16:], e.TtlSecond)
	b[20] = byte(e.OpAndDataType)
	copy(b[21:], e.Value)

	return b
}

func FromBytes(b []byte) *Entry {

	return &Entry{
		PartitionHash: binary.LittleEndian.Uint64(b[0:8]),
		UpdatedAtNs:   binary.LittleEndian.Uint64(b[8:16]),
		TtlSecond:     binary.LittleEndian.Uint32(b[16:20]),
		OpAndDataType: OpAndDataType(b[20]),
		Value:         b[21:],
	}

}

func GetPartitionHashFromBytes(b []byte) uint64 {
	return binary.LittleEndian.Uint64(b[0:8])
}

func (entry *Entry) IsExpired() bool {

	return entry.TtlSecond > 0 &&
		entry.UpdatedAtNs+uint64(entry.TtlSecond*1e9) < uint64(time.Now().UnixNano())

}
