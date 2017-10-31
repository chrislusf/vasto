package client

import (
	"fmt"

	"github.com/chrislusf/vasto/pb"
)

func (c *VastoClient) Delete(key []byte) error {

	conn, err := c.clusterListener.GetConnectionByPartitionKey(key)
	if err != nil {
		return err
	}
	defer conn.Close()

	request := &pb.Request{
		Delete: &pb.DeleteRequest{
			Key: key,
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
