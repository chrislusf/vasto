package binlog

import "hash/crc32"

func NewLogEntry(
	partitionHash uint64,
	updatedAtNs uint64,
	ttlSecond uint32,
	isDelete bool,
	key []byte,
	value []byte) *LogEntry {

	t := &LogEntry{
		PartitionHash:      partitionHash,
		UpdatedNanoSeconds: updatedAtNs,
		TtlSecond:          ttlSecond,
		IsDelete:           isDelete,
		Key:                key,
		Value:              value,
	}

	t.setCrc()

	return t
}

func (t *LogEntry) setCrc() {

	h := crc32.NewIEEE()
	h.Write(t.Key)
	h.Write(t.Value)
	t.Crc = h.Sum32()

}

func (t *LogEntry) IsValid() bool {

	h := crc32.NewIEEE()
	h.Write(t.Key)
	h.Write(t.Value)

	return t.Crc == h.Sum32()

}
