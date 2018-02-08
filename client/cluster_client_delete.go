package client

import (
	"fmt"

	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/topology"
	"github.com/chrislusf/vasto/util"
)

func (c *ClusterClient) Delete(key []byte, options ...topology.AccessOption) error {

	// TODO use partition key here

	partitionHash := util.Hash(key)

	request := &pb.Request{
		Delete: &pb.DeleteRequest{
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
		return fmt.Errorf("delete error: %v", err)
	}

	return nil
}
