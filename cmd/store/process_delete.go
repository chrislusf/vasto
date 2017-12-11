package store

import (
	"fmt"
	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/storage/binlog"
	"time"
)

func (ss *storeServer) processDelete(nodes []*shard, deleteRequest *pb.DeleteRequest) *pb.DeleteResponse {

	replica := int(deleteRequest.Replica)
	if replica >= len(nodes) {
		return &pb.DeleteResponse{
			Status: fmt.Sprintf("replica %d not found", replica),
		}
	}

	resp := &pb.DeleteResponse{
		Ok: true,
	}

	node := nodes[replica]
	err := node.db.Delete(deleteRequest.Key)
	if err != nil {
		resp.Ok = false
		resp.Status = err.Error()
	} else {
		node.logDelete(deleteRequest.Key, deleteRequest.PartitionHash, uint64(time.Now().UnixNano()))
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
