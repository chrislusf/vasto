package vs

import (
	"fmt"
	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/util"
)

func (c *ClusterClient) GetFloat64(key *KeyObject) (float64, error) {

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
		return 0, fmt.Errorf("get float64 error: %v", err)
	}

	if response.Get.Status != "" {
		return 0, fmt.Errorf(response.Get.Status)
	}

	kv := response.Get.KeyValue
	if kv == nil {
		return 0, NotFoundError
	}

	if len(kv.Value) != 8 {
		return 0, WrongDataFormatError
	}

	return util.BytesToFloat64(kv.Value), nil

}

func (c *ClusterClient) AddFloat64(key *KeyObject, value float64) error {

	var requests []*pb.Request
	request := &pb.Request{
		Merge: &pb.MergeRequest{
			Key:           key.GetKey(),
			PartitionHash: key.GetPartitionHash(),
			OpAndDataType: pb.OpAndDataType_FLOAT64,
			Value:         util.Float64ToBytes(value),
		},
	}
	requests = append(requests, request)

	return c.batchProcess(requests, func(responses []*pb.Response, err error) error {
		return err
	})
}

// PutMaxFloat64 sets a float64 value to the key, and when getting by the key, the maximum value of all previous values
// will be returned.
func (c *ClusterClient) PutMaxFloat64(key *KeyObject, value float64) error {

	var requests []*pb.Request
	request := &pb.Request{
		Merge: &pb.MergeRequest{
			Key:           key.GetKey(),
			PartitionHash: key.GetPartitionHash(),
			OpAndDataType: pb.OpAndDataType_MAX_FLOAT64,
			Value:         util.Float64ToBytes(value),
		},
	}
	requests = append(requests, request)

	return c.batchProcess(requests, func(responses []*pb.Response, err error) error {
		return err
	})
}

// PutMinFloat64 sets a float64 value to the key, and when getting by the key, the mininum value of all previous values
// will be returned.
func (c *ClusterClient) PutMinFloat64(key *KeyObject, value float64) error {

	var requests []*pb.Request
	request := &pb.Request{
		Merge: &pb.MergeRequest{
			Key:           key.GetKey(),
			PartitionHash: key.GetPartitionHash(),
			OpAndDataType: pb.OpAndDataType_MIN_FLOAT64,
			Value:         util.Float64ToBytes(value),
		},
	}
	requests = append(requests, request)

	return c.batchProcess(requests, func(responses []*pb.Response, err error) error {
		return err
	})
}
