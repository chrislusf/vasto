package gateway

import (
	"fmt"
	"log"
	"net"
	"time"

	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/util"
	"github.com/golang/protobuf/proto"
)

func (gs *gatewayServer) testTcpPut() error {

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

	for i := int64(0); i < N; i++ {
		_, err = sendRequest(conn, request)
		if err != nil {
			log.Printf("put error: %v", err)
			return err
		}
	}

	taken := time.Now().Sub(start)
	fmt.Printf("%d put average taken %d ns/op", N, taken.Nanoseconds()/N)

	return nil
}

func sendRequest(conn net.Conn, request *pb.Request) (*pb.Response, error) {

	var input, output []byte
	var err error

	input, err = proto.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %v", err)
	}

	if err = util.WriteMessage(conn, input); err != nil {
		return nil, fmt.Errorf("write request: %v", err)
	}

	output, err = util.ReadMessage(conn)

	if err != nil {
		return nil, fmt.Errorf("read response: %v", err)
	}

	response := &pb.Response{}
	if err = proto.Unmarshal(output, response); err != nil {
		return nil, fmt.Errorf("unmarshal response: %v", err)
	}

	return response, nil

}
