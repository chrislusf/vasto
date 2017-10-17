package client

import (
	"fmt"
	"log"
	"net"
	"time"

	"github.com/chrislusf/vasto/pb"
)

func (c *VastoClient) Put(key, value []byte) error {

	address := "localhost:8279"

	conn, err := net.Dial("tcp", address)
	if err != nil {
		return fmt.Errorf("Fail to dial read %s: %v", address, err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Time{})
	if c, ok := conn.(*net.TCPConn); ok {
		c.SetKeepAlive(true)
	}

	request := &pb.Request{
		Put: &pb.PutRequest{
			KeyValue: &pb.KeyValue{
				Key:   []byte("asdf"),
				Value: []byte("asdf"),
			},
			TimestampNs: 0,
			TtlMs:       0,
		},
	}

	start := time.Now()
	N := int64(100000)

	fmt.Printf("%d tcp put test start\n", N)

	requests := &pb.Requests{}
	requests.Requests = append(requests.Requests, request)

	for i := int64(0); i < N; i++ {
		_, err = pb.SendRequest(conn, requests)
		if err != nil {
			log.Printf("put error: %v", err)
			return err
		}
	}

	taken := time.Now().Sub(start)
	fmt.Printf("%d put average taken %v %d ns/op\n", N, taken, taken.Nanoseconds()/N)

	return nil
}
