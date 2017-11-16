package store

import (
	"fmt"
	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/storage/binlog"
	"time"
)

func (ss *storeServer) processDelete(deleteRequest *pb.DeleteRequest) *pb.DeleteResponse {
	replica := int(deleteRequest.Replica)
	if replica >= len(ss.nodes) {
		return &pb.DeleteResponse{
			Status: fmt.Sprintf("replica %d not found", replica),
		}
	}

	resp := &pb.DeleteResponse{
		Ok: true,
	}

	node := ss.nodes[replica]
	err := node.db.Delete(deleteRequest.Key)
	if err != nil {
		resp.Ok = false
		resp.Status = err.Error()
	} else {
		node.logDelete(deleteRequest.Key, deleteRequest.PartitionHash, uint64(time.Now().UnixNano()))
	}
	return resp

}

func (n *node) logDelete(key []byte, partitionHash uint64, updatedAtNs uint64) {

	if n.lm == nil {
		return
	}

	n.lm.AppendEntry(binlog.NewLogEntry(
		partitionHash,
		updatedAtNs,
		0,
		true,
		key,
		nil,
	))

}
