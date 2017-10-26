package change_log

import "encoding/binary"

type LogEntry struct {
	PartitionHash      uint64
	UpdatedNanoSeconds uint64
	TtlSecond          uint32
	IsDelete           bool
	Key                []byte
	Value              []byte
}

func (e *LogEntry) ToBytes() []byte {
	b := make([]byte, 24+len(e.Key)+len(e.Value))

	binary.LittleEndian.PutUint64(b, e.PartitionHash)
	binary.LittleEndian.PutUint64(b[8:], e.UpdatedNanoSeconds)
	binary.LittleEndian.PutUint32(b[16:], e.TtlSecond)
	if e.IsDelete {
		// use the highest bit of TtlSecond if it is a deletion
		b[19] |= 0xA0
	}
	lenKey := uint32(len(e.Key))
	binary.LittleEndian.PutUint32(b[20:], lenKey)
	copy(b[24:], e.Key)
	copy(b[24+len(e.Key):], e.Value)

	return b
}

func FromBytes(b []byte) *LogEntry {

	e := &LogEntry{}

	e.PartitionHash = binary.LittleEndian.Uint64(b[0:8])
	e.UpdatedNanoSeconds = binary.LittleEndian.Uint64(b[8:16])
	e.TtlSecond = binary.LittleEndian.Uint32(b[16:20])
	if b[19] & 0xA0 == 0xA0 {
		// if is deletion, the TtlSecond would not be useful
		e.IsDelete = true
	}
	lenKey := binary.LittleEndian.Uint32(b[20:24])
	e.Key = b[24 : 24+int(lenKey)]
	e.Value = b[24+int(lenKey):]

	return e
}
