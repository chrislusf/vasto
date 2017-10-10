package client

import (
	"context"
	"fmt"
	"io"
	"log"

	"github.com/chrislusf/vasto/pb"
	"google.golang.org/grpc"
)

func (c *VastoClient) registerClientAtMasterServer(msgChan chan *pb.ClientMessage) error {
	grpcConnection, err := grpc.Dial(*c.option.Master, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("fail to dial: %v", err)
	}
	defer grpcConnection.Close()

	masterClient := pb.NewVastoMasterClient(grpcConnection)

	stream, err := masterClient.RegisterClient(context.Background())
	if err != nil {
		log.Printf("RegisterClient error: %v", err)
		return err
	}

	log.Printf("register to master %s", *c.option.Master)

	clientHeartbeat := &pb.ClientHeartbeat{}

	// log.Printf("Reporting allocated %v", as.allocatedResource)

	if err := stream.Send(clientHeartbeat); err != nil {
		log.Printf("client send heartbeat: %v", err)
		return err
	}

	defer stream.CloseSend()

	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			// read done.
			return nil
		}
		if err != nil {
			return fmt.Errorf("receive topology : %v", err)
		}
		msgChan <- msg
		log.Printf("Received message %v", msg)
	}

}
