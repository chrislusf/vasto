package client

import (
	"fmt"

	"errors"
	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/util"
)

var (
	NotFoundError = errors.New("NotFound")
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

	_, err = pb.SendRequests(conn, requests)
	if err != nil {
		return fmt.Errorf("put error: %v", err)
	}

	return nil
}

func (c *VastoClient) Get(key []byte) ([]byte, error) {

	n := c.cluster.GetNode(c.cluster.FindBucket(util.Hash(key)))

	node, ok := n.(*nodeWithConnPool)

	if !ok {
		return nil, fmt.Errorf("unexpected node %+v", n)
	}

	conn, err := node.GetConnection()
	if err != nil {
		return nil, fmt.Errorf("GetConnection %+v", err)
	}
	defer conn.Close()

	request := &pb.Request{
		Get: &pb.GetRequest{
			Key: key,
		},
	}

	requests := &pb.Requests{}
	requests.Requests = append(requests.Requests, request)

	responses, err := pb.SendRequests(conn, requests)
	if err != nil {
		return nil, fmt.Errorf("put error: %v", err)
	}

	if len(responses.Responses) == 0 {
		return nil, NotFoundError
	}

	response := responses.Responses[0]
	kv := response.Get.KeyValue

	return kv.Value, nil
}
