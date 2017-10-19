package gateway

import (
	"net"

	// "github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/pb"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

func (ms *gatewayServer) serveGrpc(listener net.Listener) {
	grpcServer := grpc.NewServer()
	pb.RegisterVastoReaderServer(grpcServer, ms)
	pb.RegisterVastoWriterServer(grpcServer, ms)
	grpcServer.Serve(listener)
}

func (ms *gatewayServer) Put(ctx context.Context, putRequest *pb.PutRequest) (*pb.PutResponse, error) {
	key := putRequest.KeyValue.Key
	value := putRequest.KeyValue.Value

	resp := &pb.PutResponse{
		Ok: true,
	}
	err := ms.vastoClient.Put(key, value)
	if err != nil {
		resp.Ok = false
		resp.Status = err.Error()
	}
	return resp, nil
}

func (ms *gatewayServer) Get(ctx context.Context, getRequest *pb.GetRequest) (*pb.GetResponse, error) {
	key := getRequest.Key

	resp := &pb.GetResponse{}

	if value, err := ms.vastoClient.Get(key); err != nil {
		resp.Ok = false
		resp.Status = err.Error()
	} else {
		resp.Ok = true
		resp.KeyValue = &pb.KeyValue{
			Key:   key,
			Value: value,
		}
	}
	return resp, nil
}
