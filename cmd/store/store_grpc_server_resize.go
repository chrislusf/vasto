package store

import (
	"fmt"
	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/topology"
	"golang.org/x/net/context"
	"os"
	"github.com/golang/glog"
)

// 1. create the new or missing shards, bootstrap the data, one-time follows, and regular follows.
func (ss *storeServer) ResizePrepare(ctx context.Context, request *pb.ResizeCreateShardRequest) (*pb.ResizeCreateShardResponse, error) {

	glog.V(1).Infof("resize prepare %v", request)
	err := ss.resizeCreateShards(ctx, request)
	if err != nil {
		glog.Errorf("resize prepare %v: %v", request, err)
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

	glog.V(1).Infof("resize commit %v", request)
	err := ss.resizeCommitShardInfoNewCluster(ctx, request)
	if err != nil {
		glog.Errorf("resize commit %v: %v", request, err)
		return &pb.ResizeCommitResponse{
			Error: err.Error(),
		}, nil
	}

	return &pb.ResizeCommitResponse{
		Error: "",
	}, nil

}

// 3. cleanup old shards, and stop one-time follows
func (ss *storeServer) ResizeCleanup(ctx context.Context, request *pb.ResizeCleanupRequest) (*pb.ResizeCleanupResponse, error) {

	glog.V(1).Infof("cleanup old shards %v", request)
	err := ss.deleteOldShardsInNewCluster(ctx, request)
	if err != nil {
		glog.Errorf("cleanup old shards %v: %v", request, err)
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
				glog.V(1).Infof("adjuting shard %v to cluster size %d", shardInfo.String(), request.TargetClusterSize)
				hasChanges = true
			}
			if shardInfo.IsCandidate == true {
				shardInfo.IsCandidate = false
				glog.V(1).Infof("adjuting candidate shard %v to normal shard", shardInfo.String())
				hasChanges = true
			}
		}
	}

	if hasChanges {
		if err = ss.saveClusterConfig(localShardsStatus, request.Keyspace); err != nil {
			return err
		}
	}

	shards, found := ss.keyspaceShards.getShards(request.Keyspace)
	if !found {
		return fmt.Errorf("unexpected shards not found for %s", request.Keyspace)
	}

	for _, shard := range shards {
		if topology.IsShardInLocal(int(shard.id), int(shard.serverId), int(request.TargetClusterSize), shard.cluster.ReplicationFactor()) {
			shard.setCompactionFilterClusterSize(int(request.TargetClusterSize))
		}
	}

	return
}

func (ss *storeServer) deleteOldShardsInNewCluster(ctx context.Context, request *pb.ResizeCleanupRequest) (err error) {

	// do not notify master of the deleted shards

	// physically delete the shards
	shards, found := ss.keyspaceShards.getShards(request.Keyspace)
	if !found {
		return nil
	}

	for _, shard := range shards {
		if shard.oneTimeFollowCancel != nil {
			glog.V(1).Infof("shard %v cancels one-time following", shard)
			shard.oneTimeFollowCancel()
		}
		if !topology.IsShardInLocal(int(shard.id), int(shard.serverId), int(request.TargetClusterSize), shard.cluster.ReplicationFactor()) {
			ss.shutdownShard(shard)
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
			ss.clusterListener.RemoveKeyspace(request.Keyspace)
		} else {
			for _, shard := range shards {
				if !topology.IsShardInLocal(int(shard.id), int(shard.serverId), int(request.TargetClusterSize), shard.cluster.ReplicationFactor()) {
					delete(localShardsStatus.ShardMap, uint32(shard.id))
				}
			}
			localShardsStatus.ClusterSize = request.TargetClusterSize
			ss.saveClusterConfig(localShardsStatus, request.Keyspace)
		}
	}

	return nil
}
