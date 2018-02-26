package codec

import (
	"encoding/binary"
	"time"
)

// OpAndDataType maps to pb.OpAndDataType
type OpAndDataType byte

type Entry struct {
	PartitionHash uint64
	UpdatedAtNs   uint64
	TtlSecond     uint32
	OpAndDataType OpAndDataType
	Value         []byte
}

// ToBytes serializes the entry into bytes
func (e *Entry) ToBytes() []byte {
	b := make([]byte, len(e.Value)+21)

	binary.LittleEndian.PutUint64(b, e.PartitionHash)
	binary.LittleEndian.PutUint64(b[8:], e.UpdatedAtNs)
	binary.LittleEndian.PutUint32(b[16:], e.TtlSecond)
	b[20] = byte(e.OpAndDataType)
	copy(b[21:], e.Value)

	return b
}

// FromBytes deserialize bytes into one Entry
func FromBytes(b []byte) *Entry {

	return &Entry{
		PartitionHash: binary.LittleEndian.Uint64(b[0:8]),
		UpdatedAtNs:   binary.LittleEndian.Uint64(b[8:16]),
		TtlSecond:     binary.LittleEndian.Uint32(b[16:20]),
		OpAndDataType: OpAndDataType(b[20]),
		Value:         b[21:],
	}

}

// GetPartitionHashFromBytes reads the partition hash directly from bytes
func GetPartitionHashFromBytes(b []byte) uint64 {
	return binary.LittleEndian.Uint64(b[0:8])
}

// IsExpired checks whether the entry updated_at time plus ttl time is less than current time.
// If ttlSecond is 0, the entry will not expire.
func (e *Entry) IsExpired() bool {

	return e.TtlSecond > 0 &&
		e.UpdatedAtNs+uint64(e.TtlSecond*1e9) < uint64(time.Now().UnixNano())

}
