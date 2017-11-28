package client

import (
	"fmt"

	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/topology"
)

func (c *VastoClient) Delete(keyspace string, key []byte, options ...topology.AccessOption) error {

	conn, replica, err := c.ClusterListener.GetConnectionByPartitionKey(keyspace, key, options...)
	if err != nil {
		return err
	}

	request := &pb.Request{
		Delete: &pb.DeleteRequest{
			Replica: uint32(replica),
			Key:     key,
		},
	}

	requests := &pb.Requests{}
	requests.Requests = append(requests.Requests, request)

	_, err = pb.SendRequests(conn, requests)
	conn.Close()
	if err != nil {
		return fmt.Errorf("put error: %v", err)
	}

	return nil
}
