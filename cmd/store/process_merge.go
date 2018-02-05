package store

import (
	"log"
	"time"

	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/storage/codec"
)

func (ss *storeServer) processMerge(shard *shard, putRequest *pb.MergeRequest) *pb.WriteResponse {

	key := putRequest.Key
	nowInNano := uint64(time.Now().UnixNano())
	entry := codec.NewMergeEntry(putRequest, nowInNano)

	resp := &pb.WriteResponse{
		Ok: true,
	}

	// log.Printf("shard %d put key: %v\n", shard.id, string(putRequest.KeyValue.Key))

	err := shard.db.Merge(key, entry.ToBytes())
	if err != nil {
		resp.Ok = false
		resp.Status = err.Error()
	} else {
		shard.logMerge(putRequest, nowInNano)
	}

	return resp
}

func (s *shard) logMerge(putRequest *pb.MergeRequest, updatedAtNs uint64) {

	// println("logMerge1", putRequest.String())

	if s.lm == nil {
		return
	}

	// println("logMerge2", putRequest.String())

	err := s.lm.AppendEntry(&pb.LogEntry{
		UpdatedAtNs: updatedAtNs,
		Merge:         putRequest,
	})

	if err != nil {
		log.Printf("append put log entry: %v", err)
	}

	// println("logMerge3", putRequest.String())

}
