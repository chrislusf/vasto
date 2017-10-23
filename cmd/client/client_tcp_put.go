package client

import (
	"fmt"

	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/util"
)

func (c *VastoClient) Put(partitionKey, key, value []byte) error {

	if partitionKey == nil {
		partitionKey = key
	}
	partitionHash := util.Hash(partitionKey)

	bucket := c.cluster.FindBucket(partitionHash)

	n := c.cluster.GetNode(bucket)

	node, ok := n.(*nodeWithConnPool)

	if !ok {
		return fmt.Errorf("unexpected node %+v", n)
	}

	conn, err := node.GetConnection()
	if err != nil {
		return fmt.Errorf("GetConnection node %d %s %+v", n.GetId(), n.GetAddress(), err)
	}
	defer conn.Close()

	request := &pb.Request{
		Put: &pb.PutRequest{
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
