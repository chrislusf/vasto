package client

import (
	"errors"
	"fmt"

	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/topology"
)

var (
	NotFoundError = errors.New("not found")
)

func (c *ClusterClient) Get(key *KeyObject, options ...topology.AccessOption) ([]byte, error) {

	request := &pb.Request{
		Get: &pb.GetRequest{
			Key:           key.GetKey(),
			PartitionHash: key.GetPartitionHash(),
		},
	}

	var response *pb.Response
	err := c.batchProcess([]*pb.Request{request}, options, func(responses [] *pb.Response, err error) error {
		if err != nil {
			return err
		}
		if len(responses) == 0 {
			return NotFoundError
		}
		response = responses[0]
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("get error: %v", err)
	}

	if response.Get.Status != "" {
		return nil, fmt.Errorf(response.Get.Status)
	}

	kv := response.Get.KeyValue
	if kv == nil {
		return nil, NotFoundError
	}

	return kv.Value, nil
}
