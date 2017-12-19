package store

import (
	"github.com/chrislusf/vasto/pb"
	"golang.org/x/net/context"
	"log"
)

// ReplicateNode
// 1. if the shard is already created, do nothing
func (ss *storeServer) ReplicateNode(ctx context.Context, request *pb.ReplicateNodeRequest) (*pb.ReplicateNodeResponse, error) {

	log.Printf("replace server %v", request)
	err := ss.replicateNode(request)
	if err != nil {
		log.Printf("replace server %v: %v", request, err)
		return &pb.ReplicateNodeResponse{
			Error: err.Error(),
		}, nil
	}

	return &pb.ReplicateNodeResponse{
		Error: "",
	}, nil

}

func (ss *storeServer) replicateNode(request *pb.ReplicateNodeRequest) (err error) {

	err = ss.createShards(request.Keyspace, int(request.ServerId), int(request.ClusterSize), int(request.ReplicationFactor), true, pb.ShardInfo_READY)
	if err != nil {
		return
	}

	return nil
}
