package cluster_listener

import (
	"log"
	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/topology"
)

type ClusterEventLogger struct {
}

func (l *ClusterEventLogger) OnShardCreateEvent(cluster *topology.ClusterRing, resource *pb.StoreResource, shardStatus *pb.ShardStatus) {
	log.Printf("+ dc %s keyspace %s node %d shard %d %s cluster %s", resource.DataCenter,
		shardStatus.KeyspaceName, shardStatus.NodeId, shardStatus.ShardId, resource.Address, cluster)
}

func (l *ClusterEventLogger) OnShardUpdateEvent(cluster *topology.ClusterRing, resource *pb.StoreResource, shardStatus *pb.ShardStatus, oldShardStatus *pb.ShardStatus) {
	if oldShardStatus == nil {
	} else if oldShardStatus.Status != shardStatus.Status {
		log.Printf("* dc %s keyspace %s node %d shard %d %s cluster %s status:%s=>%s", resource.DataCenter,
			shardStatus.KeyspaceName, shardStatus.NodeId, shardStatus.ShardId, resource.GetAddress(), cluster,
			oldShardStatus.Status, shardStatus.Status)
	}
}
func (l *ClusterEventLogger) OnShardRemoveEvent(cluster *topology.ClusterRing, resource *pb.StoreResource, shardStatus *pb.ShardStatus) {
	log.Printf("- dc %s keyspace %s node %d shard %d %s cluster %s", resource.DataCenter,
		shardStatus.KeyspaceName, shardStatus.NodeId, shardStatus.ShardId, resource.Address, cluster)
}
