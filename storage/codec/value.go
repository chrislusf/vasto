package codec

import (
	"encoding/binary"
	"time"
)

type EntryValueType byte

const (
	// Spell out the entry type and values
	BYTES   EntryValueType = 0
	FLOAT64 EntryValueType = 1
)

type Entry struct {
	PartitionHash uint64
	UpdatedAtNs   uint64
	TtlSecond     uint32
	Type          EntryValueType
	Value         []byte
}

func (e *Entry) ToBytes() []byte {
	b := make([]byte, len(e.Value)+21)

	binary.LittleEndian.PutUint64(b, e.PartitionHash)
	binary.LittleEndian.PutUint64(b[8:], e.UpdatedAtNs)
	binary.LittleEndian.PutUint32(b[16:], e.TtlSecond)
	b[20] = byte(e.Type)
	copy(b[21:], e.Value)

	return b
}

func FromBytes(b []byte) *Entry {

	e := &Entry{}

	e.PartitionHash = binary.LittleEndian.Uint64(b[0:8])
	e.UpdatedAtNs = binary.LittleEndian.Uint64(b[8:16])
	e.TtlSecond = binary.LittleEndian.Uint32(b[16:20])
	e.Type = EntryValueType(b[20])
	e.Value = b[21:]

	return e
}

func GetPartitionHashFromBytes(b []byte) uint64 {
	return binary.LittleEndian.Uint64(b[0:8])
}

func (entry *Entry) IsExpired() bool {

	return entry.TtlSecond > 0 &&
		entry.UpdatedAtNs+uint64(entry.TtlSecond*1e9) < uint64(time.Now().UnixNano())

}
