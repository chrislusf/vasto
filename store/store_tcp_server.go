package store

import (
	"fmt"
	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/util"
	"github.com/golang/protobuf/proto"
	"log"
	"net"
	"time"
)

// Run starts the heartbeating to master and starts accepting requests.
func (ss *storeServer) serveTcp(listener net.Listener) {

	for {
		// Listen for an incoming connection.
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting: ", err.Error())
			continue
		}
		// Handle connections in a new goroutine.
		go func() {
			defer conn.Close()
			if err = conn.SetDeadline(time.Time{}); err != nil {
				fmt.Printf("Failed to set timeout: %v\n", err)
			}
			if c, ok := conn.(*net.TCPConn); ok {
				c.SetKeepAlive(true)
			}
			ss.handleConnection(conn)
		}()
	}
}

func (ss *storeServer) handleConnection(conn net.Conn) {

	var input, output []byte
	var err error

	input, err = util.ReadMessage(conn)

	if err != nil {
		log.Printf("Failed to read command:%v", err)
		return
	}

	request := &pb.Request{}
	if err = proto.Unmarshal(input, request); err != nil {
		log.Printf("Unmarshal error: ", err)
		return
	}

	response := ss.handleRequest(request)

	output, err = proto.Marshal(response)
	if err != nil {
		log.Printf("Marshal error: ", err)
		return
	}

	err = util.WriteMessage(conn, output)
	if err != nil {
		log.Printf("WriteMessage error: ", err)
		return
	}
}

func (ss *storeServer) handleRequest(command *pb.Request) *pb.Response {
	return &pb.Response{}
}
