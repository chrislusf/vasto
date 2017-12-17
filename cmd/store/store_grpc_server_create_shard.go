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
	err := ss.createShards(request.Keyspace, int(request.ShardId), int(request.ClusterSize), int(request.ReplicationFactor))
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

func (ss *storeServer) createShards(keyspace string, serverId int, clusterSize, replicationFactor int) (err error) {

	cluster := ss.clusterListener.AddNewKeyspace(keyspace, clusterSize, replicationFactor)
	log.Printf("new cluster: %v", cluster)

	_, found := ss.keyspaceShards.getShards(keyspace)
	if found {
		return nil
	}

	status := &pb.StoreStatusInCluster{
		Id:                uint32(serverId),
		ShardStatuses:     make(map[uint32]*pb.ShardStatus),
		ClusterSize:       uint32(clusterSize),
		ReplicationFactor: uint32(replicationFactor),
	}

	shards := topology.LocalShards(serverId, clusterSize, replicationFactor)

	for _, shard := range shards {

		shardStatus := &pb.ShardStatus{
			NodeId:            uint32(serverId),
			ShardId:           uint32(shard.ShardId),
			KeyspaceName:      keyspace,
			ClusterSize:       uint32(clusterSize),
			ReplicationFactor: uint32(replicationFactor),
		}

		ss.startShardDaemon(shardStatus, false)

		status.ShardStatuses[uint32(shard.ShardId)] = shardStatus

		ss.sendShardStatusToMaster(shardStatus)

	}

	ss.saveClusterConfig(status, keyspace)

	return nil
}

func (ss *storeServer) startExistingNodes(keyspaceName string, storeStatus *pb.StoreStatusInCluster) {
	for _, shardStatus := range storeStatus.ShardStatuses {
		ss.startShardDaemon(shardStatus, *ss.option.Bootstrap)
	}
}

func (ss *storeServer) startShardDaemon(shardStatus *pb.ShardStatus, needBootstrap bool) {

	cluster := ss.clusterListener.GetOrSetClusterRing(shardStatus.KeyspaceName, int(shardStatus.ClusterSize), int(shardStatus.ReplicationFactor))

	dir := fmt.Sprintf("%s/%s/%d", *ss.option.Dir, shardStatus.KeyspaceName, shardStatus.ShardId)
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		log.Printf("mkdir %s: %v", dir, err)
		return
	}

	ctx, node := newShard(shardStatus.KeyspaceName, dir, int(shardStatus.NodeId), int(shardStatus.ShardId), cluster, ss.clusterListener,
		int(shardStatus.ReplicationFactor), *ss.option.LogFileSizeMb, *ss.option.LogFileCount)
	// println("loading shard", node.String())
	ss.keyspaceShards.addShards(shardStatus.KeyspaceName, node)
	ss.RegisterPeriodicTask(node)
	go node.startWithBootstrapAndFollow(ctx, needBootstrap)

}
