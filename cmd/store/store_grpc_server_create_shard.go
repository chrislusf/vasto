package store

import (
	"github.com/chrislusf/vasto/pb"
	"golang.org/x/net/context"
	"log"
	"fmt"
	"os"
)

// CreateShard
// 1. if the shard is already created, do nothing
func (ss *storeServer) CreateShard(ctx context.Context, request *pb.CreateShardRequest) (*pb.CreateShardResponse, error) {

	log.Printf("create shard: %v", request)

	nodes, err := ss.createShards(request.Keyspace, int(request.ShardId), int(request.ClusterSize), int(request.ReplicationFactor))
	if err != nil {
		log.Printf("create keyspace %s: %v", request.Keyspace, err)
		return &pb.CreateShardResponse{
			Error: err.Error(),
		}, nil
	}
	ss.nodes = append(ss.nodes, nodes...)

	return &pb.CreateShardResponse{
		Error: "",
	}, nil

}

func (ss *storeServer) createShards(keyspace string, serverId int, clusterSize, replicationFactor int) (nodes []*node, err error) {

	cluster := ss.clusterListener.GetClusterRing(keyspace)

	status := &pb.StoreStatusInCluster{
		Id:                uint32(serverId),
		NodeStatuses:      make(map[uint32]*pb.ShardStatus),
		ClusterSize:       uint32(clusterSize),
		ReplicationFactor: uint32(replicationFactor),
	}

	for i := 0; i < replicationFactor; i++ {
		shardId := serverId - i
		if shardId < 0 {
			shardId += cluster.ExpectedSize()
		}
		if i != 0 && shardId == serverId {
			break
		}
		dir := fmt.Sprintf("%s/%s/%d", *ss.option.Dir, keyspace, shardId)
		err := os.MkdirAll(dir, 0755)
		if err != nil {
			return nil, fmt.Errorf("mkdir %s: %v", dir, err)
		}
		node := newNode(dir, serverId, shardId, cluster,
			replicationFactor, *ss.option.LogFileSizeMb, *ss.option.LogFileCount)
		nodes = append(nodes, node)
		if i != 0 {
			go node.start()
		}
		status.NodeStatuses[uint32(shardId)] = &pb.ShardStatus{
			Id:           uint32(shardId),
			Status:       pb.ShardStatus_EMPTY,
			KeyspaceName: keyspace,
		}
	}

	ss.saveClusterConfig(status, keyspace)
	ss.clusterListener.AddNewKeyspace(keyspace)

	// TODO register to keyspaces/datacenters

	return nodes, nil
}
