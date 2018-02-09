package store

import (
	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/storage/codec"
)

func (ss *storeServer) processPrefix(shard *shard, prefixRequest *pb.GetByPrefixRequest) *pb.GetByPrefixResponse {

	var keyValues []*pb.RawKeyValue
	resp := &pb.GetByPrefixResponse{
		Ok: true,
	}
	err := shard.db.PrefixScan(
		prefixRequest.Prefix,
		prefixRequest.LastSeenKey,
		int(prefixRequest.Limit),
		func(key, value []byte) bool {
			entry := codec.FromBytes(value)
			if !entry.IsExpired() {
				t := make([]byte, len(key))
				copy(t, key)
				keyValues = append(keyValues, &pb.RawKeyValue{
					Key:   t,
					Value: entry.Value,
				})
			}
			return true
		})
	if err != nil {
		resp.Ok = false
		resp.Status = err.Error()
	} else {
		resp.KeyValues = keyValues
	}
	return resp
}
