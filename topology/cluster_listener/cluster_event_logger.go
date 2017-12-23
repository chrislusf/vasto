package cluster_listener

import (
	"log"
	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/topology"
)

type ClusterEventLogger struct {
}

func (l *ClusterEventLogger) OnShardCreateEvent(cluster *topology.ClusterRing, resource *pb.StoreResource, ShardInfo *pb.ShardInfo) {
	log.Printf("+ dc %s keyspace %s node %d shard %d %s cluster %s", resource.DataCenter,
		ShardInfo.KeyspaceName, ShardInfo.NodeId, ShardInfo.ShardId, resource.Address, cluster)
}

func (l *ClusterEventLogger) OnShardUpdateEvent(cluster *topology.ClusterRing, resource *pb.StoreResource, ShardInfo *pb.ShardInfo, oldShardInfo *pb.ShardInfo) {
	if oldShardInfo == nil {
	} else if oldShardInfo.Status != ShardInfo.Status {
		log.Printf("* dc %s keyspace %s node %d shard %d %s cluster %s status:%s=>%s", resource.DataCenter,
			ShardInfo.KeyspaceName, ShardInfo.NodeId, ShardInfo.ShardId, resource.GetAddress(), cluster,
			oldShardInfo.Status, ShardInfo.Status)
	}
}

func (l *ClusterEventLogger) OnShardRemoveEvent(cluster *topology.ClusterRing, resource *pb.StoreResource, ShardInfo *pb.ShardInfo) {
	log.Printf("- dc %s keyspace %s node %d shard %d %s cluster %s", resource.DataCenter,
		ShardInfo.KeyspaceName, ShardInfo.NodeId, ShardInfo.ShardId, resource.Address, cluster)
}

func (l *ClusterEventLogger) OnShardPromoteEvent(cluster *topology.ClusterRing, resource *pb.StoreResource, ShardInfo *pb.ShardInfo) {
	log.Printf("=> dc %s keyspace %s node %d shard %d %s cluster %s", resource.DataCenter,
		ShardInfo.KeyspaceName, ShardInfo.NodeId, ShardInfo.ShardId, resource.Address, cluster)
}
