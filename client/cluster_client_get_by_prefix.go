package client

import (
	"fmt"

	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/topology"
)

func (c *ClusterClient) GetByPrefix(partitionKey, prefix []byte, limit uint32, lastSeenKey []byte, options ...topology.AccessOption) ([]*pb.KeyValue, error) {

	if partitionKey == nil {
		partitionKey = prefix
	}
	shardId, _ := c.ClusterListener.GetShardId(c.keyspace, partitionKey)

	conn, _, err := c.ClusterListener.GetConnectionByShardId(c.keyspace, shardId, options...)
	if err != nil {
		return nil, err
	}

	request := &pb.Request{
		ShardId: uint32(shardId),
		GetByPrefix: &pb.GetByPrefixRequest{
			Prefix:      prefix,
			Limit:       limit,
			LastSeenKey: lastSeenKey,
		},
	}

	requests := &pb.Requests{Keyspace: c.keyspace}
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
