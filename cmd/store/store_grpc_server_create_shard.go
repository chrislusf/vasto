package store

import (
	"github.com/chrislusf/vasto/pb"
	"golang.org/x/net/context"
	"log"
	"fmt"
	"os"
	"github.com/chrislusf/vasto/topology"
)

// CreateShard
// 1. if the shard is already created, do nothing
func (ss *storeServer) CreateShard(ctx context.Context, request *pb.CreateShardRequest) (*pb.CreateShardResponse, error) {

	log.Printf("create shard %v", request)
	err := ss.createShards(request.Keyspace, int(request.ServerId), int(request.ClusterSize), int(request.ReplicationFactor), false, func(shardId int) *topology.BootstrapPlan {
		return &topology.BootstrapPlan{}
	})
	if err != nil {
		log.Printf("create keyspace %s: %v", request.Keyspace, err)
		return &pb.CreateShardResponse{
			Error: err.Error(),
		}, nil
	}

	return &pb.CreateShardResponse{
		Error: "",
	}, nil

}

func (ss *storeServer) createShards(keyspace string, serverId int, clusterSize, replicationFactor int, isCandidate bool, planGen func(shardId int) *topology.BootstrapPlan) (err error) {

	ss.clusterListener.AddNewKeyspace(keyspace, clusterSize, replicationFactor)

	if _, found := ss.keyspaceShards.getShards(keyspace); found {
		localShards, foundLocalShards := ss.getServerStatusInCluster(keyspace)
		if !foundLocalShards {
			return fmt.Errorf("missing local shard status for keyspace %s", keyspace)
		}
		if int(localShards.ClusterSize) == clusterSize && int(localShards.ReplicationFactor) == replicationFactor {
			return fmt.Errorf("keyspace %s already exists", keyspace)
		}
		if serverId != int(localShards.Id) {
			return fmt.Errorf("local server id = %d, not matching requested server id %d", localShards.Id, serverId)
		}
	}

	localShards := ss.getOrCreateServerStatusInCluster(keyspace, serverId, clusterSize, replicationFactor)

	for _, clusterShard := range topology.LocalShards(serverId, clusterSize, replicationFactor) {

		if _, found := localShards.ShardMap[uint32(clusterShard.ShardId)]; found {
			// skip existing shards
			continue
		}

		shardInfo := &pb.ShardInfo{
			ServerId:          uint32(serverId),
			ShardId:           uint32(clusterShard.ShardId),
			KeyspaceName:      keyspace,
			ClusterSize:       uint32(clusterSize),
			ReplicationFactor: uint32(replicationFactor),
			IsCandidate:       isCandidate,
		}

		ss.bootstrapShard(shardInfo, planGen(clusterShard.ShardId))

		localShards.ShardMap[uint32(clusterShard.ShardId)] = shardInfo

		ss.sendShardInfoToMaster(shardInfo, pb.ShardInfo_READY)

	}

	ss.saveClusterConfig(localShards, keyspace)

	return nil
}

func (ss *storeServer) startExistingNodes(keyspaceName string, storeStatus *pb.LocalShardsInCluster) {
	for _, shardInfo := range storeStatus.ShardMap {
		ss.bootstrapShard(shardInfo, &topology.BootstrapPlan{
			IsNormalStart:                true,
			IsNormalStartBootstrapNeeded: *ss.option.Bootstrap,
		})
	}
}

func (ss *storeServer) bootstrapShard(shardInfo *pb.ShardInfo, bootstrapOption *topology.BootstrapPlan) {

	cluster := ss.clusterListener.GetOrSetClusterRing(shardInfo.KeyspaceName, int(shardInfo.ClusterSize), int(shardInfo.ReplicationFactor))

	dir := fmt.Sprintf("%s/%s/%d", *ss.option.Dir, shardInfo.KeyspaceName, shardInfo.ShardId)
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		log.Printf("mkdir %s: %v", dir, err)
		return
	}

	ctx, shard := newShard(shardInfo.KeyspaceName, dir, int(shardInfo.ServerId), int(shardInfo.ShardId), cluster, ss.clusterListener,
		int(shardInfo.ReplicationFactor), *ss.option.LogFileSizeMb, *ss.option.LogFileCount)
	// println("loading shard", shard.String())
	ss.keyspaceShards.addShards(shardInfo.KeyspaceName, shard)
	ss.RegisterPeriodicTask(shard)
	shard.startWithBootstrapPlan(ctx, bootstrapOption, ss.selfAdminAddress())

}
