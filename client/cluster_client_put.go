package client

import (
	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/topology"
)

func (c *ClusterClient) Put(key *KeyObject, value []byte, options ...topology.AccessOption) error {

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

	return c.batchProcess(requests, options, func(responses [] *pb.Response, err error) error {
		return err
	})
}

func (c *ClusterClient) Append(key *KeyObject, value []byte, options ...topology.AccessOption) error {

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

	return c.batchProcess(requests, options, func(responses [] *pb.Response, err error) error {
		return err
	})
}

func (c *ClusterClient) BatchPut(rows []*Row, options ...topology.AccessOption) error {

	var requests []*pb.Request
	for _, row := range rows {
		request := &pb.Request{
			Put: &pb.PutRequest{
				Key:           row.Key.GetKey(),
				PartitionHash: row.Key.GetPartitionHash(),
				TtlSecond:     0,
				OpAndDataType: pb.OpAndDataType_BYTES,
				Value:         row.Value,
			},
		}
		requests = append(requests, request)
	}

	return c.batchProcess(requests, options, func(responses [] *pb.Response, err error) error {
		return err
	})
}
