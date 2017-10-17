package store

import (
	"context"
	"fmt"
	"log"

	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/util"
	"google.golang.org/grpc"
	"io"
	"time"
)

func (ss *storeServer) keepConnectedToMasterServer() {

	util.RetryForever(func() error {
		return ss.registerAtMasterServer()
	}, 2*time.Second)

}

func (ss *storeServer) registerAtMasterServer() error {
	grpcConnection, err := grpc.Dial(*ss.option.Master, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("fail to dial: %v", err)
	}
	defer grpcConnection.Close()

	client := pb.NewVastoMasterClient(grpcConnection)

	stream, err := client.RegisterStore(context.Background())
	if err != nil {
		log.Printf("SendHeartbeat error: %v", err)
		return err
	}

	log.Printf("register to master %s", *ss.option.Master)

	storeHeartbeat := &pb.StoreHeartbeat{
		DataCenter: *ss.option.DataCenter,
		Store: &pb.StoreResource{
			Id: *ss.option.Id,
			Address: fmt.Sprintf(
				"%s:%d",
				*ss.option.Host,
				int32(*ss.option.TcpPort),
			),
		},
	}

	// log.Printf("Reporting allocated %v", as.allocatedResource)

	if err := stream.Send(storeHeartbeat); err != nil {
		log.Printf("RegisterStore (%+v) = %v", storeHeartbeat, err)
		return err
	}

	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			// read done.
			return nil
		}
		if err != nil {
			return fmt.Errorf("receive topology : %v", err)
		}
		ss.processStoreMessage(msg)
	}

	return nil

}

func (ss *storeServer) processStoreMessage(msg *pb.StoreMessage) {
	log.Printf("Received message %v", msg)
}
