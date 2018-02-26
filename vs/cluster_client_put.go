package vs

import (
	"github.com/chrislusf/vasto/pb"
)

// Put puts one key value pair to one partition
func (c *ClusterClient) Put(key *KeyObject, value []byte) error {

	var requests []*pb.Request
	request := &pb.Request{
		Put: &pb.PutRequest{
			Key:           key.GetKey(),
			PartitionHash: key.GetPartitionHash(),
			UpdatedAtNs:   c.UpdatedAtNs,
			TtlSecond:     c.TtlSecond,
			OpAndDataType: pb.OpAndDataType_BYTES,
			Value:         value,
		},
	}
	requests = append(requests, request)

	return c.BatchProcess(requests, func(responses []*pb.Response, err error) error {
		return err
	})
}

func (c *ClusterClient) Append(key *KeyObject, value []byte) error {

	var requests []*pb.Request
	request := &pb.Request{
		Merge: &pb.MergeRequest{
			Key:           key.GetKey(),
			PartitionHash: key.GetPartitionHash(),
			UpdatedAtNs:   c.UpdatedAtNs,
			OpAndDataType: pb.OpAndDataType_BYTES,
			Value:         value,
		},
	}
	requests = append(requests, request)

	return c.BatchProcess(requests, func(responses []*pb.Response, err error) error {
		return err
	})
}

// BatchPut puts the key value pairs to different partitions
func (c *ClusterClient) BatchPut(rows []*KeyValue) error {

	var requests []*pb.Request
	for _, row := range rows {
		request := &pb.Request{
			Put: &pb.PutRequest{
				Key:           row.KeyObject.GetKey(),
				PartitionHash: row.KeyObject.GetPartitionHash(),
				UpdatedAtNs:   c.UpdatedAtNs,
				TtlSecond:     c.TtlSecond,
				OpAndDataType: pb.OpAndDataType_BYTES,
				Value:         row.GetValue(),
			},
		}
		requests = append(requests, request)
	}

	return c.BatchProcess(requests, func(responses []*pb.Response, err error) error {
		return err
	})
}
