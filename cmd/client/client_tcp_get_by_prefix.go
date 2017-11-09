package client

import (
	"fmt"

	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/topology"
)

func (c *VastoClient) GetByPrefix(partitionKey, prefix []byte, limit uint32, lastSeenKey []byte, options ...topology.AccessOption) ([]*pb.KeyValue, error) {

	if partitionKey == nil {
		partitionKey = prefix
	}

	conn, replica, err := c.clusterListener.GetConnectionByPartitionKey(partitionKey, options...)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	request := &pb.Request{
		GetByPrefix: &pb.GetByPrefixRequest{
			Replica:     uint32(replica),
			Prefix:      prefix,
			Limit:       limit,
			LastSeenKey: lastSeenKey,
		},
	}

	requests := &pb.Requests{}
	requests.Requests = append(requests.Requests, request)

	responses, err := pb.SendRequests(conn, requests)
	if err != nil {
		return nil, fmt.Errorf("get error: %v", err)
	}

	if len(responses.Responses) == 0 {
		return nil, NotFoundError
	}

	response := responses.Responses[0]

	return response.GetByPrefix.KeyValues, nil
}
