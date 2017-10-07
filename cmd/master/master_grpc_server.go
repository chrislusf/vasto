package master

import (
	"fmt"
	"io"
	"net"

	"github.com/chrislusf/vasto/pb"
	"google.golang.org/grpc"
)

func (ms *masterServer) serveGrpc(listener net.Listener) {
	grpcServer := grpc.NewServer()
	pb.RegisterVastoMasterServer(grpcServer, ms)
	grpcServer.Serve(listener)
}

func (ms *masterServer) RegisterStore(stream pb.VastoMaster_RegisterStoreServer) error {

	var heartbeat, lastHeartbeat *pb.StoreHeartbeat
	var err error

	for {
		heartbeat, err = stream.Recv()
		if err != nil {
			break
		}
		lastHeartbeat = heartbeat
		fmt.Printf("received store %+v\n", heartbeat.Store)
	}

	if err == io.EOF {
		fmt.Printf("store %+v disconnected normally.\n", lastHeartbeat.Store)
		return nil
	}
	if err != nil {
		fmt.Printf("store %+v broke: %v.\n", lastHeartbeat.Store, err)
		return err
	}

	return nil
}

func (ms *masterServer) RegisterGateway(stream pb.VastoMaster_RegisterGatewayServer) error {
	return nil
}
