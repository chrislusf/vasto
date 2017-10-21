package gateway

import (
	"bufio"
	"fmt"
	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/util"
	"github.com/golang/protobuf/proto"
	"io"
	"log"
	"net"
	"time"
)

// Run starts the heartbeating to master and starts accepting requests.
func (ms *gatewayServer) serveTcp(listener net.Listener) {

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
				c.SetNoDelay(true)
				c.SetLinger(0)
			}
			ms.handleConnection(conn)
		}()
	}
}

func (ms *gatewayServer) handleConnection(conn net.Conn) {

	reader := bufio.NewReader(conn)

	for {
		if err := ms.handleRequest(reader, conn); err != nil {
			if err != io.EOF {
				log.Printf("handleRequest: %v", err)
			}
			return
		}
	}

}

func (ms *gatewayServer) handleRequest(reader io.Reader, writer io.Writer) error {

	var input, output []byte
	var err error

	input, err = util.ReadMessage(reader)

	if err == io.EOF {
		return err
	}
	if err != nil {
		return fmt.Errorf("read message: %v", err)
	}

	requests := &pb.Requests{}
	if err = proto.Unmarshal(input, requests); err != nil {
		return fmt.Errorf("unmarshal: %v", err)
	}

	responses := &pb.Responses{}
	for _, request := range requests.Requests {
		response := ms.processRequest(request)
		responses.Responses = append(responses.Responses, response)
	}

	output, err = proto.Marshal(responses)
	if err != nil {
		return fmt.Errorf("marshal: %v", err)
	}

	err = util.WriteMessage(writer, output)
	if err != nil {
		return fmt.Errorf("write message: %v", err)
	}

	return nil

}

func (ms *gatewayServer) processRequest(command *pb.Request) *pb.Response {
	if command.GetGet() != nil {
		key := command.Get.Key
		if value, err := ms.vastoClient.Get(key); err != nil {
			return &pb.Response{
				Get: &pb.GetResponse{
					Status: err.Error(),
				},
			}
		} else {
			return &pb.Response{
				Get: &pb.GetResponse{
					Ok: true,
					KeyValue: &pb.KeyValue{
						Key:   key,
						Value: value,
					},
				},
			}
		}
	} else if command.GetPut() != nil {
		key := command.Put.KeyValue.Key
		value := command.Put.KeyValue.Value

		resp := &pb.PutResponse{
			Ok: true,
		}
		err := ms.vastoClient.Put(key, value)
		if err != nil {
			resp.Ok = false
			resp.Status = err.Error()
		}
		return &pb.Response{
			Put: resp,
		}
	} else if command.GetDelete() != nil {
		key := command.Delete.Key

		resp := &pb.DeleteResponse{
			Ok: true,
		}
		err := ms.vastoClient.Delete(key)
		if err != nil {
			resp.Ok = false
			resp.Status = err.Error()
		}
		return &pb.Response{
			Delete: resp,
		}
	}
	return &pb.Response{
		Put: &pb.PutResponse{
			Ok: true,
		},
	}
}
