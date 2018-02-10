package client

import (
	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/topology"
	"github.com/chrislusf/vasto/util"
	"fmt"
)

func (c *ClusterClient) GetFloat64(key *keyObject, options ...topology.AccessOption) (float64, error) {

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
		return 0, fmt.Errorf("get float64 error: %v", err)
	}

	if response.Get.Status != "" {
		return 0, fmt.Errorf(response.Get.Status)
	}

	kv := response.Get.KeyValue
	if kv == nil {
		return 0, NotFoundError
	}

	return util.BytesToFloat64(kv.Value), nil

}

func (c *ClusterClient) AddFloat64(key *keyObject, value float64, options ...topology.AccessOption) error {

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

	return c.batchProcess(requests, options, func(responses [] *pb.Response, err error) error {
		return err
	})
}

// Max sets a value to the key, and when getting the key, the maximum value of all previous keys
// will be returned.
func (c *ClusterClient) MaxFloat64(key *keyObject, value float64, options ...topology.AccessOption) error {

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

	return c.batchProcess(requests, options, func(responses [] *pb.Response, err error) error {
		return err
	})
}

// Min sets a value to the key, and when getting the key, the mininum value of all previous keys
// will be returned.
func (c *ClusterClient) MinFloat64(key *keyObject, value float64, options ...topology.AccessOption) error {

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

	return c.batchProcess(requests, options, func(responses [] *pb.Response, err error) error {
		return err
	})
}
