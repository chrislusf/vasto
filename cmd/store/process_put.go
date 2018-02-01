package store

import (
	"log"
	"time"

	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/storage/codec"
)

func (ss *storeServer) processPut(shard *shard, putRequest *pb.PutRequest) *pb.WriteResponse {

	key := putRequest.KeyValue.Key
	nowInNano := uint64(time.Now().UnixNano())
	entry := codec.NewPutEntry(putRequest, nowInNano)

	resp := &pb.WriteResponse{
		Ok: true,
	}

	// log.Printf("shard %d put key: %v\n", shard.id, string(putRequest.KeyValue.Key))

	err := shard.db.Put(key, entry.ToBytes())
	if err != nil {
		resp.Ok = false
		resp.Status = err.Error()
	} else {
		shard.logPut(putRequest, nowInNano)
	}

	return resp
}

func (s *shard) logPut(putRequest *pb.PutRequest, updatedAtNs uint64) {

	// println("logPut1", putRequest.String())

	if s.lm == nil {
		return
	}

	// println("logPut2", putRequest.String())

	err := s.lm.AppendEntry(&pb.LogEntry{
		UpdatedAtNs: updatedAtNs,
		Put:         putRequest,
	})

	if err != nil {
		log.Printf("append put log entry: %v", err)
	}

	// println("logPut3", putRequest.String())

}
