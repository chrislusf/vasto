package store

import (
	"context"
	"fmt"
	"log"

	"github.com/chrislusf/vasto/pb"
	"google.golang.org/grpc"
)

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

	storeResource := &pb.StoreResource{
		Id: 123,
		Location: &pb.Location{
			Server: *ss.option.Host,
			Port:   int32(*ss.option.TcpPort),
		},
	}

	// log.Printf("Reporting allocated %v", as.allocatedResource)

	if err := stream.Send(&pb.StoreHeartbeat{storeResource}); err != nil {
		log.Printf("RegisterStore (%+v) = %v", storeResource, err)
		return err
	}

	select {}

}
