package cluster_listener

import (
	"context"
	"fmt"
	"io"
	"log"

	"github.com/chrislusf/vasto/pb"
	"google.golang.org/grpc"
)

func (c *ClusterListener) registerClientAtMasterServer(master string, keyspace, dataCenter string,
	msgChan chan *pb.ClientMessage) error {
	grpcConnection, err := grpc.Dial(master, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("fail to dial %s: %v", master, err)
	}
	defer grpcConnection.Close()

	masterClient := pb.NewVastoMasterClient(grpcConnection)

	stream, err := masterClient.RegisterClient(context.Background())
	if err != nil {
		return fmt.Errorf("register client on master %v: %v", master, err)
	}

	clientHeartbeat := &pb.ClientHeartbeat{
		Keyspace:   keyspace,
		DataCenter: dataCenter,
	}

	// log.Printf("Reporting allocated %v", as.allocatedResource)

	if err := stream.Send(clientHeartbeat); err != nil {
		return fmt.Errorf("client send heartbeat: %v", err)
	}

	log.Printf("register client to master %s", master)

	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			// read done.
			return nil
		}
		if err != nil {
			return fmt.Errorf("client receive topology : %v", err)
		}
		msgChan <- msg
		// log.Printf("client received message %v", msg)
	}

}
