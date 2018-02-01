package codec

import "github.com/chrislusf/vasto/pb"

func NewPutEntry(put *pb.PutRequest, updatedAtNs uint64) *Entry {
	return &Entry{
		PartitionHash: put.PartitionHash,
		UpdatedAtNs:   updatedAtNs,
		TtlSecond:     put.TtlSecond,
		Type:          BYTES,
		Value:         put.KeyValue.Value,
	}
}
