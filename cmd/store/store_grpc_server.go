package store

import (
	"net"

	"github.com/chrislusf/vasto/pb"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

func (ss *storeServer) serveGrpc(listener net.Listener) {
	grpcServer := grpc.NewServer()
	pb.RegisterVastoStoreServer(grpcServer, ss)
	grpcServer.Serve(listener)
}

func (ss *storeServer) Put(ctx context.Context, request *pb.PutRequest) (*pb.PutResponse, error) {
	return &pb.PutResponse{}, nil
}
