package store

import (
	"github.com/chrislusf/vasto/topology"
	"github.com/chrislusf/vasto/pb"
	"log"
)

// the following functions implements cluster_listener.ShardEventProcessor

func (s *shard) OnShardCreateEvent(cluster *topology.ClusterRing, resource *pb.StoreResource, shardStatus *pb.ShardStatus) {
	log.Printf("+ keyspace %s node %d shard %d cluster %s",
		shardStatus.KeyspaceName, shardStatus.NodeId, shardStatus.ShardId, cluster)
	if s.keyspace != shardStatus.KeyspaceName {
		return
	}
	if s.id == int(shardStatus.ShardId) {
		return
	}
	log.Printf("+ peer keyspace %s shard %d found peer shard %d",
		shardStatus.KeyspaceName, s.id, shardStatus.ShardId)
}

func (s *shard) OnShardUpdateEvent(cluster *topology.ClusterRing, resource *pb.StoreResource, shardStatus *pb.ShardStatus, oldShardStatus *pb.ShardStatus) {
	if oldShardStatus == nil {
	} else if oldShardStatus.Status != shardStatus.Status {
		log.Printf("+ keyspace %s node %d shard %d cluster %s status:%s=>%s",
			shardStatus.KeyspaceName, shardStatus.NodeId, shardStatus.ShardId, cluster, oldShardStatus.Status, shardStatus.Status)
	}
}
func (s *shard) OnShardRemoveEvent(cluster *topology.ClusterRing, resource *pb.StoreResource, shardStatus *pb.ShardStatus) {
	log.Printf("- keyspace %s node %d shard %d cluster %s",
		shardStatus.KeyspaceName, shardStatus.NodeId, shardStatus.ShardId, cluster)
	log.Printf("- dc %s keyspace %s node %d shard %d %s cluster %s", resource.DataCenter,
		shardStatus.KeyspaceName, shardStatus.NodeId, shardStatus.ShardId, resource.Address, cluster)
}
