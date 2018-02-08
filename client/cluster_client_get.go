package client

import (
	"errors"
	"fmt"

	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/topology"
	"github.com/chrislusf/vasto/util"
)

var (
	NotFoundError = errors.New("NotFound")
)

func (c *ClusterClient) Get(key []byte, options ...topology.AccessOption) ([]byte, error) {

	partitionHash := util.Hash(key)

	request := &pb.Request{
		Get: &pb.GetRequest{
			Key:           key,
			PartitionHash: partitionHash,
		},
	}

	var response *pb.Response
	err := c.batchProcess([]*pb.Request{request}, func(responses [] *pb.Response, err error) error {
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
