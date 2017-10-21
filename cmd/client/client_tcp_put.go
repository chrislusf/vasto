package client

import (
	"fmt"

	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/util"
)

func (c *VastoClient) Put(key, value []byte) error {

	bucket := c.cluster.FindBucket(util.Hash(key))

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
			TimestampNs: 0,
			TtlMs:       0,
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
