package store

import (
	"fmt"
	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/topology"
	"golang.org/x/net/context"
	"github.com/golang/glog"
)

// ReplicateNodePrepare
// if the shard is already created, do nothing
// 1. create the new shard and follow the old shard and its peers
func (ss *storeServer) ReplicateNodePrepare(ctx context.Context, request *pb.ReplicateNodePrepareRequest) (*pb.ReplicateNodePrepareResponse, error) {

	glog.V(1).Infof("replicate shard prepare %v", request)
	err := ss.replicateNode(request)
	if err != nil {
		glog.Errorf("replicate shard prepare %v: %v", request, err)
		return &pb.ReplicateNodePrepareResponse{
			Error: err.Error(),
		}, nil
	}

	return &pb.ReplicateNodePrepareResponse{
		Error: "",
	}, nil

}

// 2. let the server to promote the new shard from CANDIDATE to READY
func (ss *storeServer) ReplicateNodeCommit(ctx context.Context, request *pb.ReplicateNodeCommitRequest) (*pb.ReplicateNodeCommitResponse, error) {

	glog.V(1).Infof("replicate shard commit %v", request)
	err := ss.setShardStatus(request)
	if err != nil {
		glog.Errorf("replicate shard commit %v: %v", request, err)
		return &pb.ReplicateNodeCommitResponse{
			Error: err.Error(),
		}, nil
	}

	return &pb.ReplicateNodeCommitResponse{
		Error: "",
	}, nil

}

// 4. let the server to remove the old shard
func (ss *storeServer) ReplicateNodeCleanup(ctx context.Context, request *pb.ReplicateNodeCleanupRequest) (*pb.ReplicateNodeCleanupResponse, error) {

	glog.V(1).Infof("cleanup shard %v", request)
	err := ss.deleteShards(request.Keyspace, false)
	if err != nil {
		glog.Errorf("cleanup shard %v: %v", request, err)
		return &pb.ReplicateNodeCleanupResponse{
			Error: err.Error(),
		}, nil
	}

	return &pb.ReplicateNodeCleanupResponse{
		Error: "",
	}, nil

}

func (ss *storeServer) replicateNode(request *pb.ReplicateNodePrepareRequest) (err error) {

	err = ss.createShards(request.Keyspace, int(request.ServerId), int(request.ClusterSize), int(request.ReplicationFactor), true, func(shardId int) *topology.BootstrapPlan {

		return topology.BootstrapPlanWithTopoChange(&topology.BootstrapRequest{
			ServerId:          int(request.ServerId),
			ShardId:           shardId,
			FromClusterSize:   int(request.ClusterSize),
			ToClusterSize:     int(request.ClusterSize),
			ReplicationFactor: int(request.ReplicationFactor),
		})

	})
	if err != nil {
		return
	}

	return nil
}

func (ss *storeServer) setShardStatus(request *pb.ReplicateNodeCommitRequest) (err error) {

	localShardsStatus, found := ss.getServerStatusInCluster(request.Keyspace)
	if !found {
		return fmt.Errorf("not found keyspace %s", request.Keyspace)
	}

	for _, shardInfo := range localShardsStatus.ShardMap {
		shardInfo.IsCandidate = false
	}

	return ss.saveClusterConfig(localShardsStatus, request.Keyspace)
}
