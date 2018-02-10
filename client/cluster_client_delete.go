package client

import (
	"fmt"

	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/topology"
)

func (c *ClusterClient) Delete(key *KeyObject, options ...topology.AccessOption) error {

	request := &pb.Request{
		Delete: &pb.DeleteRequest{
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
		return fmt.Errorf("delete error: %v", err)
	}

	return nil
}
