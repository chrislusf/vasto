package vs

import (
	"errors"
	"fmt"

	"github.com/chrislusf/vasto/pb"
)

var (
	NotFoundError        = errors.New("not found")
	WrongDataFormatError = errors.New("wrong data format")
)

func (c *ClusterClient) Get(key *KeyObject) ([]byte, error) {

	request := &pb.Request{
		Get: &pb.GetRequest{
			Key:           key.GetKey(),
			PartitionHash: key.GetPartitionHash(),
		},
	}

	var response *pb.Response
	err := c.batchProcess([]*pb.Request{request}, func(responses []*pb.Response, err error) error {
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
