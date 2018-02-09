package store

import (
	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/storage/codec"
)

func (ss *storeServer) processGet(shard *shard, getRequest *pb.GetRequest) *pb.GetResponse {
	key := getRequest.Key
	// println("replica", replica, "shard", shards[replica].id, "keyspace", shards[replica].keyspace, "server", shards[replica].serverId, "request", getRequest.String())
	if b, err := shard.db.Get(key); err != nil {
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
			KeyValue: &pb.KeyTypeValue{
				Key:      key,
				DataType: pb.OpAndDataType(entry.OpAndDataType),
				Value:    entry.Value,
			},
		}
	}
}
