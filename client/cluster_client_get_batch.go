package client

import (
	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/topology"
	"github.com/chrislusf/vasto/util"
)

type answer struct {
	keyvalues []*pb.KeyValue
	err       error
}

func (c *ClusterClient) BatchGet(keys [][]byte, options ...topology.AccessOption) ([]*pb.KeyValue, error) {

	var requests []*pb.Request

	for _, key := range keys {
		request := &pb.Request{
			Get: &pb.GetRequest{
				Key:           key,
				PartitionHash: util.Hash(key),
			},
		}
		requests = append(requests, request)
	}

	var ret []*pb.KeyValue
	err := c.batchProcess(requests, func(responses [] *pb.Response, err error) error {
		if err != nil {
			return err
		}
		for _, response := range responses {
			ret = append(ret, response.Get.KeyValue)
		}
		return nil
	})

	return ret, err
}
