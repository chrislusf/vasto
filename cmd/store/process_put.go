package store

import (
	"fmt"
	"log"
	"time"

	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/storage/binlog"
	"github.com/chrislusf/vasto/storage/codec"
)

func (ss *storeServer) processPut(putRequest *pb.PutRequest) *pb.PutResponse {
	replica := int(putRequest.Replica)
	if replica >= len(ss.nodes) {
		return &pb.PutResponse{
			Status: fmt.Sprintf("replica %d not found", replica),
		}
	}

	key := putRequest.KeyValue.Key
	nowInNano := uint64(time.Now().UnixNano())
	entry := &codec.Entry{
		PartitionHash: putRequest.PartitionHash,
		UpdatedAtNs:   nowInNano,
		TtlSecond:     putRequest.TtlSecond,
		Value:         putRequest.KeyValue.Value,
	}

	resp := &pb.PutResponse{
		Ok: true,
	}

	node := ss.nodes[replica]

	fmt.Printf("node %d put keyValue: %v\n", node.id, putRequest.KeyValue.String())

	err := node.db.Put(key, entry.ToBytes())
	if err != nil {
		resp.Ok = false
		resp.Status = err.Error()
	} else {
		node.logPut(putRequest, nowInNano)
	}

	return resp
}

func (n *node) logPut(putRequest *pb.PutRequest, updatedAtNs uint64) {

	// println("logPut1", putRequest.String())

	if n.lm == nil {
		return
	}

	// println("logPut2", putRequest.String())

	err := n.lm.AppendEntry(binlog.NewLogEntry(
		putRequest.PartitionHash,
		updatedAtNs,
		putRequest.TtlSecond,
		false,
		putRequest.KeyValue.Key,
		putRequest.KeyValue.Value,
	))

	if err != nil {
		log.Printf("append log entry: %v", err)
	}

	// println("logPut3", putRequest.String())

}
