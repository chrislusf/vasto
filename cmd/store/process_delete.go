package store

import (
	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/storage/binlog"
	"time"
)

func (ss *storeServer) processDelete(shard *shard, deleteRequest *pb.DeleteRequest) *pb.DeleteResponse {

	resp := &pb.DeleteResponse{
		Ok: true,
	}

	err := shard.db.Delete(deleteRequest.Key)
	if err != nil {
		resp.Ok = false
		resp.Status = err.Error()
	} else {
		shard.logDelete(deleteRequest.Key, deleteRequest.PartitionHash, uint64(time.Now().UnixNano()))
	}
	return resp

}

func (s *shard) logDelete(key []byte, partitionHash uint64, updatedAtNs uint64) {

	if s.lm == nil {
		return
	}

	s.lm.AppendEntry(binlog.NewLogEntry(
		partitionHash,
		updatedAtNs,
		0,
		true,
		key,
		nil,
	))

}
