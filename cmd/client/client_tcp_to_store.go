package client

import (
	"fmt"

	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/util"
)

func (c *VastoClient) Put(key, value []byte) error {

	n := c.cluster.GetNode(c.cluster.FindBucket(util.Hash(key)))

	node, ok := n.(*nodeWithConnPool)

	if !ok {
		return fmt.Errorf("unexpected node %+v", n)
	}

	conn, err := node.GetConnection()
	if err != nil {
		return fmt.Errorf("GetConnection %+v", err)
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

	_, err = pb.SendRequest(conn, requests)
	if err != nil {
		return fmt.Errorf("put error: %v", err)
	}

	return nil
}
