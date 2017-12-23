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
	err := ss.createShards(request.Keyspace, int(request.ShardId), int(request.ClusterSize), int(request.ReplicationFactor), false, false, pb.ShardInfo_READY)
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

func (ss *storeServer) createShards(keyspace string, serverId int, clusterSize, replicationFactor int, isCandidate bool, needBootstrap bool, shardStatus pb.ShardInfo_Status) (err error) {

	cluster := ss.clusterListener.AddNewKeyspace(keyspace, clusterSize, replicationFactor)
	log.Printf("new cluster: %v", cluster)

	_, found := ss.keyspaceShards.getShards(keyspace)
	if found {
		return nil
	}

	status := &pb.LocalShardsInCluster{
		Id:                uint32(serverId),
		ShardMap:          make(map[uint32]*pb.ShardInfo),
		ClusterSize:       uint32(clusterSize),
		ReplicationFactor: uint32(replicationFactor),
	}

	shards := topology.LocalShards(serverId, clusterSize, replicationFactor)

	for _, shard := range shards {

		shardInfo := &pb.ShardInfo{
			NodeId:            uint32(serverId),
			ShardId:           uint32(shard.ShardId),
			KeyspaceName:      keyspace,
			ClusterSize:       uint32(clusterSize),
			ReplicationFactor: uint32(replicationFactor),
			IsCandidate:       isCandidate,
		}

		ss.startShardDaemon(shardInfo, needBootstrap)

		status.ShardMap[uint32(shard.ShardId)] = shardInfo

		ss.sendShardInfoToMaster(shardInfo, shardStatus)

	}

	ss.saveClusterConfig(status, keyspace)

	return nil
}

func (ss *storeServer) startExistingNodes(keyspaceName string, storeStatus *pb.LocalShardsInCluster) {
	for _, shardInfo := range storeStatus.ShardMap {
		ss.startShardDaemon(shardInfo, *ss.option.Bootstrap)
	}
}

func (ss *storeServer) startShardDaemon(shardInfo *pb.ShardInfo, needBootstrap bool) {

	cluster := ss.clusterListener.GetOrSetClusterRing(shardInfo.KeyspaceName, int(shardInfo.ClusterSize), int(shardInfo.ReplicationFactor))

	dir := fmt.Sprintf("%s/%s/%d", *ss.option.Dir, shardInfo.KeyspaceName, shardInfo.ShardId)
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		log.Printf("mkdir %s: %v", dir, err)
		return
	}

	ctx, node := newShard(shardInfo.KeyspaceName, dir, int(shardInfo.NodeId), int(shardInfo.ShardId), cluster, ss.clusterListener,
		int(shardInfo.ReplicationFactor), *ss.option.LogFileSizeMb, *ss.option.LogFileCount)
	// println("loading shard", node.String())
	ss.keyspaceShards.addShards(shardInfo.KeyspaceName, node)
	ss.RegisterPeriodicTask(node)
	go node.startWithBootstrapAndFollow(ctx, ss.selfAdminAddress(), needBootstrap)

}
