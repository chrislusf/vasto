package store

import (
	"github.com/chrislusf/vasto/topology"
	"github.com/chrislusf/vasto/pb"
	"log"
)

// the following functions implements cluster_listener.ShardEventProcessor

func (s *shard) OnShardCreateEvent(cluster *topology.ClusterRing, resource *pb.StoreResource, ShardInfo *pb.ShardInfo) {
	log.Printf("+ keyspace %s node %d shard %d cluster %s",
		ShardInfo.KeyspaceName, ShardInfo.NodeId, ShardInfo.ShardId, cluster)
	if s.keyspace != ShardInfo.KeyspaceName {
		return
	}
	if s.id == int(ShardInfo.ShardId) {
		return
	}
	log.Printf("+ peer keyspace %s shard %d found peer shard %d",
		ShardInfo.KeyspaceName, s.id, ShardInfo.ShardId)
}

func (s *shard) OnShardUpdateEvent(cluster *topology.ClusterRing, resource *pb.StoreResource, ShardInfo *pb.ShardInfo, oldShardInfo *pb.ShardInfo) {
	if oldShardInfo == nil {
	} else if oldShardInfo.Status != ShardInfo.Status {
		log.Printf("+ keyspace %s node %d shard %d cluster %s status:%s=>%s",
			ShardInfo.KeyspaceName, ShardInfo.NodeId, ShardInfo.ShardId, cluster, oldShardInfo.Status, ShardInfo.Status)
	}
}
func (s *shard) OnShardRemoveEvent(cluster *topology.ClusterRing, resource *pb.StoreResource, ShardInfo *pb.ShardInfo) {
	log.Printf("- keyspace %s node %d shard %d cluster %s",
		ShardInfo.KeyspaceName, ShardInfo.NodeId, ShardInfo.ShardId, cluster)
	log.Printf("- dc %s keyspace %s node %d shard %d %s cluster %s", resource.DataCenter,
		ShardInfo.KeyspaceName, ShardInfo.NodeId, ShardInfo.ShardId, resource.Address, cluster)
}
