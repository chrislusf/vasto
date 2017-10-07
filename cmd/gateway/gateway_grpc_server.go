package gateway

import (
	"net"

	// "github.com/chrislusf/vasto/pb"
	"google.golang.org/grpc"
)

func (ms *gatewayServer) serveGrpc(listener net.Listener) {
	grpcServer := grpc.NewServer()
	// pb.Re(grpcServer, ms)
	grpcServer.Serve(listener)
}
