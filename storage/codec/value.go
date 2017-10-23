package codec

import "encoding/binary"

type Entry struct {
	PartitionHash uint64
	UpdatedSecond uint32
	TtlSecond     uint32
	Value         []byte
}

func (e *Entry) ToBytes() []byte {
	b := make([]byte, len(e.Value)+16)

	binary.LittleEndian.PutUint64(b, e.PartitionHash)
	binary.LittleEndian.PutUint32(b[8:], e.UpdatedSecond)
	binary.LittleEndian.PutUint32(b[12:], e.TtlSecond)
	copy(b[16:], e.Value)

	return b
}

func FromBytes(b []byte) *Entry {

	e := &Entry{}

	e.PartitionHash = binary.LittleEndian.Uint64(b[0:8])
	e.UpdatedSecond = binary.LittleEndian.Uint32(b[8:12])
	e.TtlSecond = binary.LittleEndian.Uint32(b[12:16])
	e.Value = b[16:]

	return e
}
