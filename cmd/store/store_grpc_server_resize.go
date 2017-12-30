package store

import (
	"github.com/chrislusf/vasto/pb"
	"golang.org/x/net/context"
	"log"
	"github.com/chrislusf/vasto/topology"
)

// 1. create the new or missing shards, bootstrap the data, one-time follows, and regular follows.
func (ss *storeServer) ResizeCreateShard(ctx context.Context, request *pb.ResizeCreateShardRequest) (*pb.ResizeCreateShardResponse, error) {

	log.Printf("resize create shards %v", request)
	err := ss.resizeCreateShards(ctx, request)
	if err != nil {
		log.Printf("resize create shards %v: %v", request, err)
		return &pb.ResizeCreateShardResponse{
			Error: err.Error(),
		}, nil
	}

	return &pb.ResizeCreateShardResponse{
		Error: "",
	}, nil

}

// 2. bootstrap the new shards
// TODO delete this
func (ss *storeServer) ResizeBootstrap(ctx context.Context, request *pb.ResizeBootstrapRequest) (*pb.ResizeBootstrapResponse, error) {

	log.Printf("shrink cluster prepare %v", request)
	err := ss.resizeCreateShards(ctx, nil)
	if err != nil {
		log.Printf("shrink cluster prepare %v: %v", request, err)
		return &pb.ResizeBootstrapResponse{
			Error: err.Error(),
		}, nil
	}

	return &pb.ResizeBootstrapResponse{
		Error: "",
	}, nil

}

func (ss *storeServer) resizeCreateShards(ctx context.Context, request *pb.ResizeCreateShardRequest) (err error) {

	err = ss.createShards(request.Keyspace, int(request.ServerId), int(request.TargetClusterSize), int(request.ReplicationFactor), true, func(shardId int) *topology.BootstrapPlan {

		return topology.BootstrapPlanWithTopoChange(&topology.BootstrapRequest{
			ServerId:          int(request.ServerId),
			ShardId:           shardId,
			FromClusterSize:   int(request.ClusterSize),
			ToClusterSize:     int(request.TargetClusterSize),
			ReplicationFactor: int(request.ReplicationFactor),
		})

	})
	if err != nil {
		return
	}

	return nil
}
