package store

import (
	"time"

	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/storage/codec"
	"github.com/chrislusf/glog"
)

func (ss *storeServer) processPut(shard *shard, putRequest *pb.PutRequest) *pb.WriteResponse {

	key := putRequest.Key
	nowInNano := uint64(time.Now().UnixNano())
	entry := codec.NewPutEntry(putRequest, nowInNano)

	resp := &pb.WriteResponse{
		Ok: true,
	}

	// glog.V(2).Infof"shard %d put key: %v\n", shard.id, string(putRequest.KeyValue.Key))

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
		glog.Errorf("append put log entry: %v", err)
	}

	// println("logPut3", putRequest.String())

}
