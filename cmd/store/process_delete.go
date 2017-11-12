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
	err := ss.nodes[replica].db.Delete(deleteRequest.Key)
	if err != nil {
		resp.Ok = false
		resp.Status = err.Error()
	} else {
		ss.logDelete(deleteRequest.Key, deleteRequest.PartitionHash, uint64(time.Now().UnixNano()))
	}
	return resp

}

func (ss *storeServer) logDelete(key []byte, partitionHash uint64, updatedAtNs uint64) {

	if ss.nodes[0].lm == nil {
		return
	}

	ss.nodes[0].lm.AppendEntry(binlog.NewLogEntry(
		partitionHash,
		updatedAtNs,
		0,
		true,
		key,
		nil,
	))

}
