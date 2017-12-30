package store

import (
	"fmt"
	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/storage/codec"
)

func (ss *storeServer) processGet(shards []*shard, getRequest *pb.GetRequest) *pb.GetResponse {
	replica := int(getRequest.Replica)
	if replica >= len(shards) {
		return &pb.GetResponse{
			Status: fmt.Sprintf("replica %d not found", replica),
		}
	}
	key := getRequest.Key
	// println("replica", replica, "shard", shards[replica].id, "keyspace", shards[replica].keyspace, "server", shards[replica].serverId, "request", getRequest.String())
	if b, err := shards[replica].db.Get(key); err != nil {
		return &pb.GetResponse{
			Status: err.Error(),
		}
	} else if len(b) == 0 {
		return &pb.GetResponse{
			Ok: true,
		}
	} else {
		entry := codec.FromBytes(b)
		if entry.IsExpired() {
			return &pb.GetResponse{
				Ok:     false,
				Status: "expired",
			}
		}
		return &pb.GetResponse{
			Ok: true,
			KeyValue: &pb.KeyValue{
				Key:   key,
				Value: entry.Value,
			},
		}
	}
}
