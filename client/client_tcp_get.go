package client

import (
	"errors"
	"fmt"

	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/topology"
)

var (
	NotFoundError = errors.New("NotFound")
)

func (c *VastoClient) Get(keyspace string, key []byte, options ...topology.AccessOption) ([]byte, error) {

	shardId, _ := c.ClusterListener.GetShardId(keyspace, key)

	conn, _, err := c.ClusterListener.GetConnectionByShardId(keyspace, shardId, options...)
	if err != nil {
		return nil, err
	}

	request := &pb.Request{
		ShardId: uint32(shardId),
		Get: &pb.GetRequest{
			Key: key,
		},
	}

	requests := &pb.Requests{Keyspace: keyspace}
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

	if response.Get.Status != "" {
		return nil, fmt.Errorf(response.Get.Status)
	}

	kv := response.Get.KeyValue
	if kv == nil {
		return nil, NotFoundError
	}

	return kv.Value, nil
}
