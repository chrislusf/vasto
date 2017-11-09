package store

import (
	"log"
	"time"

	"fmt"
	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/storage/change_log"
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
	err := ss.nodes[replica].db.Put(key, entry.ToBytes())
	if err != nil {
		resp.Ok = false
		resp.Status = err.Error()
	} else {
		ss.logPut(putRequest, nowInNano)
	}

	return resp
}

func (ss *storeServer) logPut(putRequest *pb.PutRequest, updatedAtNs uint64) {

	// println("logPut1", putRequest.String())

	if ss.nodes[0].lm == nil {
		return
	}

	// println("logPut2", putRequest.String())

	err := ss.nodes[0].lm.AppendEntry(change_log.NewLogEntry(
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
