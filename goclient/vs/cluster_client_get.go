package vs

import (
	"errors"
	"fmt"

	"github.com/chrislusf/vasto/pb"
)

var (
	// ErrorNotFound error when no matching records are found
	ErrorNotFound = errors.New("not found")
	// ErrorWrongDataFormat error when data type is unexpected
	ErrorWrongDataFormat = errors.New("wrong data format")
)

// Get gets the value bytes by the key
func (c *ClusterClient) Get(key *KeyObject) ([]byte, pb.OpAndDataType, error) {

	request := &pb.Request{
		Get: &pb.GetRequest{
			Key:           key.GetKey(),
			PartitionHash: key.GetPartitionHash(),
		},
	}

	var response *pb.Response
	err := c.BatchProcess([]*pb.Request{request}, func(responses []*pb.Response, err error) error {
		if err != nil {
			return err
		}
		if len(responses) == 0 {
			return ErrorNotFound
		}
		response = responses[0]
		return nil
	})

	if err != nil {
		return nil, pb.OpAndDataType_BYTES, fmt.Errorf("get error: %v", err)
	}

	if response.Get.Status != "" {
		return nil, pb.OpAndDataType_BYTES, fmt.Errorf(response.Get.Status)
	}

	kv := response.Get.KeyValue
	if kv == nil {
		return nil, pb.OpAndDataType_BYTES, ErrorNotFound
	}

	return kv.Value, kv.DataType, nil
}
