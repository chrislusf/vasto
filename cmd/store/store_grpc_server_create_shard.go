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
		dir := fmt.Sprintf("%s/%s/%d", *ss.option.Dir, keyspace, shard.ShardId)
		err := os.MkdirAll(dir, 0755)
		if err != nil {
			return fmt.Errorf("mkdir %s: %v", dir, err)
		}

		ctx, node := newShard(keyspace, dir, serverId, shard.ShardId, cluster, ss.clusterListener,
			replicationFactor, *ss.option.LogFileSizeMb, *ss.option.LogFileCount)

		ss.keyspaceShards.addShards(keyspace, node)
		ss.RegisterPeriodicTask(node)
		go node.startWithBootstrapAndFollow(ctx, false)

		shardStatus := &pb.ShardStatus{
			NodeId:            uint32(serverId),
			ShardId:           uint32(shard.ShardId),
			Status:            pb.ShardStatus_EMPTY,
			KeyspaceName:      keyspace,
			ClusterSize:       uint32(clusterSize),
			ReplicationFactor: uint32(replicationFactor),
		}

		log.Printf("sending shard status %v", shardStatus)

		status.ShardStatuses[uint32(shard.ShardId)] = shardStatus

		// register the shard at master
		ss.shardStatusChan <- shardStatus

	}

	ss.saveClusterConfig(status, keyspace)

	return nil
}
