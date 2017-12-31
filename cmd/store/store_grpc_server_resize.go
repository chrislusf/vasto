package store

import (
	"github.com/chrislusf/vasto/pb"
	"golang.org/x/net/context"
	"log"
	"github.com/chrislusf/vasto/topology"
	"fmt"
	"os"
)

// 1. create the new or missing shards, bootstrap the data, one-time follows, and regular follows.
func (ss *storeServer) ResizePrepare(ctx context.Context, request *pb.ResizeCreateShardRequest) (*pb.ResizeCreateShardResponse, error) {

	log.Printf("resize prepare %v", request)
	err := ss.resizeCreateShards(ctx, request)
	if err != nil {
		log.Printf("resize prepare %v: %v", request, err)
		return &pb.ResizeCreateShardResponse{
			Error: err.Error(),
		}, nil
	}

	return &pb.ResizeCreateShardResponse{
		Error: "",
	}, nil

}

// 2. commit the new shards, adjust local cluster size, status, etc, not informing the master of shard info changes
func (ss *storeServer) ResizeCommit(ctx context.Context, request *pb.ResizeCommitRequest) (*pb.ResizeCommitResponse, error) {

	log.Printf("resize commit %v", request)
	err := ss.resizeCommitShardInfoNewCluster(ctx, request)
	if err != nil {
		log.Printf("resize commit %v: %v", request, err)
		return &pb.ResizeCommitResponse{
			Error: err.Error(),
		}, nil
	}

	return &pb.ResizeCommitResponse{
		Error: "",
	}, nil

}

// 3. cleanup old shards
func (ss *storeServer) ResizeCleanup(ctx context.Context, request *pb.ResizeCleanupRequest) (*pb.ResizeCleanupResponse, error) {

	log.Printf("cleanup old shards %v", request)
	err := ss.deleteOldShardsInNewCluster(ctx, request)
	if err != nil {
		log.Printf("cleanup old shards %v: %v", request, err)
		return &pb.ResizeCleanupResponse{
			Error: err.Error(),
		}, nil
	}

	return &pb.ResizeCleanupResponse{
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

func (ss *storeServer) resizeCommitShardInfoNewCluster(ctx context.Context, request *pb.ResizeCommitRequest) (err error) {

	localShardsStatus, found := ss.getServerStatusInCluster(request.Keyspace)
	if !found {
		return fmt.Errorf("not found keyspace %s", request.Keyspace)
	}

	hasChanges := false

	for _, shardInfo := range localShardsStatus.ShardMap {
		if topology.IsShardInLocal(int(shardInfo.ShardId), int(localShardsStatus.Id), int(request.TargetClusterSize), int(localShardsStatus.ReplicationFactor)) {
			if shardInfo.ClusterSize != request.TargetClusterSize {
				shardInfo.ClusterSize = request.TargetClusterSize
				log.Printf("adjuting shard %v to cluster size %d", shardInfo.String(), request.TargetClusterSize)
				hasChanges = true
			}
			if shardInfo.IsCandidate == true {
				shardInfo.IsCandidate = false
				log.Printf("adjuting candidate shard %v to normal shard", shardInfo.String())
				hasChanges = true
			}
		}
	}

	if hasChanges {
		err = ss.saveClusterConfig(localShardsStatus, request.Keyspace)
	}

	return
}

func (ss *storeServer) deleteOldShardsInNewCluster(ctx context.Context, request *pb.ResizeCleanupRequest) (err error) {

	// do notify master of the deleted shards

	// physically delete the shards
	shards, found := ss.keyspaceShards.getShards(request.Keyspace)
	if !found {
		return nil
	}

	for _, shard := range shards {
		if !topology.IsShardInLocal(int(shard.id), int(shard.serverId), int(request.TargetClusterSize), shard.clusterRing.ReplicationFactor()) {
			ss.UnregisterPeriodicTask(shard)
			shard.shutdownNode()
			shard.db.Close()
			shard.db.Destroy()
		}
	}

	if localShardsStatus, found := ss.getServerStatusInCluster(request.Keyspace); found {
		if localShardsStatus.Id >= request.TargetClusterSize {
			// retiring server
			// remove all meta info and in-memory objects
			dir := fmt.Sprintf("%s/%s", *ss.option.Dir, request.Keyspace)
			os.RemoveAll(dir)
			ss.keyspaceShards.deleteKeyspace(request.Keyspace)
			ss.deleteServerStatusInCluster(request.Keyspace)
		}
	}

	return nil
}
