package client

import (
	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/topology"
)

func (c *ClusterClient) Put(rows []*Row, options ...topology.AccessOption) error {

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

	return c.batchProcess(requests, func(responses [] *pb.Response, err error) error {
		return err
	})
}
