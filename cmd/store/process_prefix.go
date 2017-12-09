package store

import (
	"fmt"
	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/storage/codec"
)

func (ss *storeServer) processPrefix(nodes []*node, prefixRequest *pb.GetByPrefixRequest) *pb.GetByPrefixResponse {
	replica := int(prefixRequest.Replica)
	if replica >= len(nodes) {
		return &pb.GetByPrefixResponse{
			Status: fmt.Sprintf("replica %d not found", replica),
		}
	}

	var keyValues []*pb.KeyValue
	resp := &pb.GetByPrefixResponse{
		Ok: true,
	}
	err := nodes[replica].db.PrefixScan(
		prefixRequest.Prefix,
		prefixRequest.LastSeenKey,
		int(prefixRequest.Limit),
		func(key, value []byte) bool {
			entry := codec.FromBytes(value)
			if !entry.IsExpired() {
				t := make([]byte, len(key))
				copy(t, key)
				keyValues = append(keyValues, &pb.KeyValue{
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
