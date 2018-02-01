package store

import (
	"github.com/chrislusf/vasto/pb"
	"time"
	"log"
)

func (ss *storeServer) processDelete(shard *shard, deleteRequest *pb.DeleteRequest) *pb.WriteResponse {

	resp := &pb.WriteResponse{
		Ok: true,
	}

	err := shard.db.Delete(deleteRequest.Key)
	if err != nil {
		resp.Ok = false
		resp.Status = err.Error()
	} else {
		shard.logDelete(deleteRequest, uint64(time.Now().UnixNano()))
	}
	return resp

}

func (s *shard) logDelete(deleteRequest *pb.DeleteRequest, updatedAtNs uint64) {

	if s.lm == nil {
		return
	}

	err := s.lm.AppendEntry(&pb.LogEntry{
		UpdatedAtNs: updatedAtNs,
		Delete:      deleteRequest,
	})

	if err != nil {
		log.Printf("append delete log entry: %v", err)
	}

}
