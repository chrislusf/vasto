package store

import (
	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/storage/codec"
	"time"
)

func (ss *storeServer) processPrefix(prefixRequest *pb.GetByPrefixRequest) *pb.GetByPrefixResponse {
	var keyValues []*pb.KeyValue
	resp := &pb.GetByPrefixResponse{
		Ok: true,
	}
	err := ss.db.PrefixScan(
		prefixRequest.Prefix,
		prefixRequest.LastSeenKey,
		int(prefixRequest.Limit),
		func(key, value []byte) bool {
			entry := codec.FromBytes(value)
			if entry.TtlSecond == 0 || entry.UpdatedSecond+entry.TtlSecond >= uint32(time.Now().Unix()) {
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
