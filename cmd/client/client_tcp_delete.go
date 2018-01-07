package client

import (
	"fmt"

	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/topology"
)

func (c *VastoClient) Delete(keyspace string, key []byte, options ...topology.AccessOption) error {

	// TODO use partition key here
	shardId, partitionHash := c.ClusterListener.GetShardId(keyspace, key)

	conn, _, err := c.ClusterListener.GetConnectionByShardId(keyspace, shardId, options...)
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

	requests := &pb.Requests{Keyspace: keyspace}
	requests.Requests = append(requests.Requests, request)

	_, err = pb.SendRequests(conn, requests)
	conn.Close()
	if err != nil {
		return fmt.Errorf("delete error: %v", err)
	}

	return nil
}
