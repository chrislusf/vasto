package store

import (
	"net"

	"github.com/chrislusf/vasto/pb"
	"google.golang.org/grpc"
)

func (ss *storeServer) serveGrpc(listener net.Listener) {
	grpcServer := grpc.NewServer()
	pb.RegisterVastoStoreServer(grpcServer, ss)
	grpcServer.Serve(listener)
}
