package client

import (
	"fmt"

	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/topology"
)

func (c *VastoClient) GetByPrefix(keyspace string, partitionKey, prefix []byte, limit uint32, lastSeenKey []byte, options ...topology.AccessOption) ([]*pb.KeyValue, error) {

	if partitionKey == nil {
		partitionKey = prefix
	}

	conn, replica, err := c.ClusterListener.GetConnectionByPartitionKey(keyspace, partitionKey, options...)
	if err != nil {
		return nil, err
	}

	request := &pb.Request{
		GetByPrefix: &pb.GetByPrefixRequest{
			Replica:     uint32(replica),
			Prefix:      prefix,
			Limit:       limit,
			LastSeenKey: lastSeenKey,
		},
	}

	requests := &pb.Requests{Keyspace:keyspace}
	requests.Requests = append(requests.Requests, request)

	responses, err := pb.SendRequests(conn, requests)
	conn.Close()
	if err != nil {
		return nil, fmt.Errorf("get error: %v", err)
	}

	if len(responses.Responses) == 0 {
		return nil, NotFoundError
	}

	response := responses.Responses[0]

	return response.GetByPrefix.KeyValues, nil
}
