package store

import (
	"time"

	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/storage/change_log"
	"github.com/chrislusf/vasto/storage/codec"
)

func (ss *storeServer) processPut(putRequest *pb.PutRequest) *pb.PutResponse {
	key := putRequest.KeyValue.Key
	now := uint64(time.Now().UnixNano())
	entry := &codec.Entry{
		PartitionHash: putRequest.PartitionHash,
		UpdatedAtNs:   now,
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
	} else {
		ss.logPut(putRequest, now)
	}

	return resp
}

func (ss *storeServer) logPut(putRequest *pb.PutRequest, updatedAtNs uint64) {

	if ss.lm == nil {
		return
	}

	ss.lm.AddEntry(change_log.NewLogEntry(
		putRequest.PartitionHash,
		updatedAtNs,
		putRequest.TtlSecond,
		false,
		putRequest.KeyValue.Key,
		putRequest.KeyValue.Value,
	))

}
