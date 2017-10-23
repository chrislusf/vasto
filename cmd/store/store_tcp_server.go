package store

import (
	"bufio"
	"fmt"
	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/storage/codec"
	"github.com/chrislusf/vasto/util"
	"github.com/golang/protobuf/proto"
	"io"
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
				c.SetNoDelay(true)
				c.SetLinger(0)
			}
			ss.handleConnection(conn)
		}()
	}
}

func (ss *storeServer) handleConnection(conn net.Conn) {

	reader := bufio.NewReader(conn)

	for {
		if err := ss.handleRequest(reader, conn); err != nil {
			if err != io.EOF {
				log.Printf("handleRequest: %v", err)
			}
			return
		}
	}

}

func (ss *storeServer) handleRequest(reader io.Reader, writer io.Writer) error {

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
		response := ss.processRequest(request)
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

func (ss *storeServer) processRequest(command *pb.Request) *pb.Response {
	if command.GetGet() != nil {
		key := command.Get.Key
		if b, err := ss.db.Get(key); err != nil {
			return &pb.Response{
				Get: &pb.GetResponse{
					Status: err.Error(),
				},
			}
		} else if len(b) == 0 {
			return &pb.Response{
				Get: &pb.GetResponse{
					Ok: true,
				},
			}
		} else {
			entry := codec.FromBytes(b)
			if entry.TtlSecond > 0 && entry.UpdatedSecond+entry.TtlSecond < uint32(time.Now().Unix()) {
				return &pb.Response{
					Get: &pb.GetResponse{
						Ok:     false,
						Status: "expired",
					},
				}
			}
			return &pb.Response{
				Get: &pb.GetResponse{
					Ok: true,
					KeyValue: &pb.KeyValue{
						Key:   key,
						Value: entry.Value,
					},
				},
			}
		}
	} else if command.GetPut() != nil {
		key := command.Put.KeyValue.Key
		entry := &codec.Entry{
			PartitionHash: command.Put.PartitionHash,
			UpdatedSecond: uint32(time.Now().Unix()),
			TtlSecond:     command.Put.TtlSecond,
			Value:         command.Put.KeyValue.Value,
		}

		resp := &pb.PutResponse{
			Ok: true,
		}
		err := ss.db.Put(key, entry.ToBytes())
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
		err := ss.db.Delete(key)
		if err != nil {
			resp.Ok = false
			resp.Status = err.Error()
		}
		return &pb.Response{
			Delete: resp,
		}
	} else if command.GetGetByPrefix() != nil {

		var keyValues []*pb.KeyValue
		resp := &pb.DeleteResponse{
			Ok: true,
		}
		err := ss.db.PrefixScan(
			command.GetByPrefix.Prefix,
			command.GetByPrefix.LastSeenKey,
			int(command.GetByPrefix.Limit),
			func(key, value []byte) bool {
				entry := codec.FromBytes(value)
				if entry.TtlSecond == 0 || entry.UpdatedSecond+entry.TtlSecond >= uint32(time.Now().Unix()) {
					t := make([]byte, len(key))
					copy(t, key)
					keyValues = append(keyValues, &pb.KeyValue{
						Key:   t,
						Value: entry.Value,
					})
				}
				return true
			})
		if err != nil {
			resp.Ok = false
			resp.Status = err.Error()
		}
		return &pb.Response{
			GetByPrefix: &pb.GetByPrefixResponse{
				KeyValues: keyValues,
			},
		}
	}
	return &pb.Response{
		Put: &pb.PutResponse{
			Ok: true,
		},
	}
}
