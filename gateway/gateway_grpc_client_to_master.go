package gateway

import (
	"context"
	"fmt"
	"log"

	"github.com/chrislusf/vasto/pb"
	"google.golang.org/grpc"
)

func (gs *gatewayServer) registerGatewayAtMasterServer() error {
	grpcConnection, err := grpc.Dial(*gs.option.Master, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("fail to dial: %v", err)
	}
	defer grpcConnection.Close()

	client := pb.NewVastoMasterClient(grpcConnection)

	stream, err := client.RegisterGateway(context.Background())
	if err != nil {
		log.Printf("RegisterGateway error: %v", err)
		return err
	}

	log.Printf("register to master %s", *gs.option.Master)

	gatewayHeartbeat := &pb.GatewayHeartbeat{
		Location: &pb.Location{
			Server: *gs.option.Host,
			Port:   int32(*gs.option.Port),
		},
	}

	// log.Printf("Reporting allocated %v", as.allocatedResource)

	if err := stream.Send(gatewayHeartbeat); err != nil {
		log.Printf("RegisterGateway (%+v) = %v", gatewayHeartbeat, err)
		return err
	}

	// gs.testPut()
	gs.testTcpPut()

	select {}

}
