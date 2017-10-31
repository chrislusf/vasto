package client

import (
	"fmt"

	"github.com/chrislusf/vasto/pb"
)

func (c *VastoClient) GetByPrefix(partitionKey, prefix []byte, limit uint32, lastSeenKey []byte) ([]*pb.KeyValue, error) {

	if partitionKey == nil {
		partitionKey = prefix
	}

	conn, err := c.clusterListener.GetConnectionByPartitionKey(partitionKey)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	request := &pb.Request{
		GetByPrefix: &pb.GetByPrefixRequest{
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
