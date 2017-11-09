package client

import (
	"fmt"

	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/topology"
	"github.com/chrislusf/vasto/util"
)

func (c *VastoClient) Put(partitionKey, key, value []byte, options ...topology.AccessOption) error {

	if partitionKey == nil {
		partitionKey = key
	}
	partitionHash := util.Hash(partitionKey)
	conn, replica, err := c.clusterListener.GetConnectionByPartitionHash(partitionHash, options...)
	if err != nil {
		return err
	}
	defer conn.Close()

	request := &pb.Request{
		Put: &pb.PutRequest{
			Replica: uint32(replica),
			KeyValue: &pb.KeyValue{
				Key:   key,
				Value: value,
			},
			PartitionHash: partitionHash,
			TtlSecond:     0,
		},
	}

	requests := &pb.Requests{}
	requests.Requests = append(requests.Requests, request)

	_, err = pb.SendRequests(conn, requests)
	if err != nil {
		return fmt.Errorf("put error: %v", err)
	}

	return nil
}
