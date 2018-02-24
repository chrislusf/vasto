package vs

import (
	"github.com/chrislusf/vasto/pb"
)

func (c *ClusterClient) Put(key *KeyObject, value []byte) error {

	var requests []*pb.Request
	request := &pb.Request{
		Put: &pb.PutRequest{
			Key:           key.GetKey(),
			PartitionHash: key.GetPartitionHash(),
			TtlSecond:     0,
			OpAndDataType: pb.OpAndDataType_BYTES,
			Value:         value,
		},
	}
	requests = append(requests, request)

	return c.batchProcess(requests, func(responses []*pb.Response, err error) error {
		return err
	})
}

func (c *ClusterClient) Append(key *KeyObject, value []byte) error {

	var requests []*pb.Request
	request := &pb.Request{
		Merge: &pb.MergeRequest{
			Key:           key.GetKey(),
			PartitionHash: key.GetPartitionHash(),
			OpAndDataType: pb.OpAndDataType_BYTES,
			Value:         value,
		},
	}
	requests = append(requests, request)

	return c.batchProcess(requests, func(responses []*pb.Response, err error) error {
		return err
	})
}

func (c *ClusterClient) BatchPut(rows []*KeyValue) error {

	var requests []*pb.Request
	for _, row := range rows {
		request := &pb.Request{
			Put: &pb.PutRequest{
				Key:           row.KeyObject.GetKey(),
				PartitionHash: row.KeyObject.GetPartitionHash(),
				TtlSecond:     0,
				OpAndDataType: pb.OpAndDataType_BYTES,
				Value:         row.GetValue(),
			},
		}
		requests = append(requests, request)
	}

	return c.batchProcess(requests, func(responses []*pb.Response, err error) error {
		return err
	})
}
