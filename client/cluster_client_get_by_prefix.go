package client

import (
	"fmt"

	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/topology"
	"github.com/chrislusf/vasto/util"
)

func (c *ClusterClient) GetByPrefix(partitionKey, prefix []byte, limit uint32, lastSeenKey []byte, options ...topology.AccessOption) ([]*pb.KeyTypeValue, error) {

	if partitionKey == nil {
		partitionKey = prefix
	}

	request := &pb.Request{
		GetByPrefix: &pb.GetByPrefixRequest{
			Prefix:        prefix,
			PartitionHash: util.Hash(partitionKey),
			Limit:         limit,
			LastSeenKey:   lastSeenKey,
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
		return nil, fmt.Errorf("GetByPrefix error: %v", err)
	}

	return response.GetByPrefix.KeyValues, nil
}
