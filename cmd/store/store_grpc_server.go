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
	pb.RegisterVastoReaderServer(grpcServer, ss)
	pb.RegisterVastoWriterServer(grpcServer, ss)
	grpcServer.Serve(listener)
}

func (ss *storeServer) Put(ctx context.Context, putRequest *pb.PutRequest) (*pb.PutResponse, error) {
	key := putRequest.KeyValue.Key
	value := putRequest.KeyValue.Value

	resp := &pb.PutResponse{
		Ok: true,
	}
	err := ss.db.Put(key, value)
	if err != nil {
		resp.Ok = false
		resp.Status = err.Error()
	}
	return resp, nil
}
func (ss *storeServer) Copy(stream pb.VastoStore_CopyServer) error {
	return nil
}
func (ss *storeServer) CopyDone(ctx context.Context, request *pb.CopyDoneMessge) (*pb.Empty, error) {
	return &pb.Empty{}, nil
}

func (ss *storeServer) Get(ctx context.Context, getRequest *pb.GetRequest) (*pb.GetResponse, error) {
	key := getRequest.Key

	resp := &pb.GetResponse{}

	if value, err := ss.db.Get(key); err != nil {
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
