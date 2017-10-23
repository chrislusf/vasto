package store

import (
	"time"

	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/storage/codec"
)

func (ss *storeServer) processPut(putRequest *pb.PutRequest) *pb.PutResponse {
	key := putRequest.KeyValue.Key
	entry := &codec.Entry{
		PartitionHash: putRequest.PartitionHash,
		UpdatedSecond: uint32(time.Now().Unix()),
		TtlSecond:     putRequest.TtlSecond,
		Value:         putRequest.KeyValue.Value,
	}

	resp := &pb.PutResponse{
		Ok: true,
	}
	err := ss.db.Put(key, entry.ToBytes())
	if err != nil {
		resp.Ok = false
		resp.Status = err.Error()
	}
	return resp
}
