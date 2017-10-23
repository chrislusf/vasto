package store

import (
	"time"

	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/storage/codec"
)

func (ss *storeServer) processGet(getRequest *pb.GetRequest) *pb.GetResponse {
	key := getRequest.Key
	if b, err := ss.db.Get(key); err != nil {
		return &pb.GetResponse{
			Status: err.Error(),
		}
	} else if len(b) == 0 {
		return &pb.GetResponse{
			Ok: true,
		}
	} else {
		entry := codec.FromBytes(b)
		if entry.TtlSecond > 0 && entry.UpdatedSecond+entry.TtlSecond < uint32(time.Now().Unix()) {
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
