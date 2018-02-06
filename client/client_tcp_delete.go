package client

import (
	"fmt"

	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/topology"
)

func (c *ClusterClient) Delete(key []byte, options ...topology.AccessOption) error {

	// TODO use partition key here
	shardId, partitionHash := c.ClusterListener.GetShardId(c.keyspace, key)

	conn, _, err := c.ClusterListener.GetConnectionByShardId(c.keyspace, shardId, options...)
	if err != nil {
		return err
	}

	request := &pb.Request{
		ShardId: uint32(shardId),
		Delete: &pb.DeleteRequest{
			Key:           key,
			PartitionHash: partitionHash,
		},
	}

	requests := &pb.Requests{Keyspace: c.keyspace}
	requests.Requests = append(requests.Requests, request)

	_, err = pb.SendRequests(conn, requests)
	conn.Close()
	if err != nil {
		return fmt.Errorf("delete error: %v", err)
	}

	return nil
}
