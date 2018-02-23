package codec

import "github.com/chrislusf/vasto/pb"

func NewPutEntry(put *pb.PutRequest, updatedAtNs uint64) *Entry {
	return &Entry{
		PartitionHash: put.PartitionHash,
		UpdatedAtNs:   updatedAtNs,
		TtlSecond:     put.TtlSecond,
		OpAndDataType: OpAndDataType(put.OpAndDataType),
		Value:         put.Value,
	}
}

func NewMergeEntry(m *pb.MergeRequest, updatedAtNs uint64) *Entry {
	return &Entry{
		PartitionHash: m.PartitionHash,
		UpdatedAtNs:   updatedAtNs,
		TtlSecond:     0,
		OpAndDataType: OpAndDataType(m.OpAndDataType),
		Value:         m.Value,
	}
}
