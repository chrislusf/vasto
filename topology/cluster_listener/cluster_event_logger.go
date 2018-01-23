package cluster_listener

import (
	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/topology"
	"log"
)

type ClusterEventLogger struct {
}

func (l *ClusterEventLogger) OnShardCreateEvent(cluster *topology.Cluster, resource *pb.StoreResource, shardInfo *pb.ShardInfo) {
	log.Printf("+ dc %s keyspace %s node %d shard %d %s cluster %s", resource.DataCenter,
		shardInfo.KeyspaceName, shardInfo.ServerId, shardInfo.ShardId, resource.Address, cluster)
}

func (l *ClusterEventLogger) OnShardUpdateEvent(cluster *topology.Cluster, resource *pb.StoreResource, shardInfo *pb.ShardInfo, oldShardInfo *pb.ShardInfo) {
	if oldShardInfo == nil {
	} else if oldShardInfo.Status != shardInfo.Status {
		log.Printf("* dc %s %s on %s cluster %s status:%s=>%s", resource.DataCenter,
			shardInfo.IdentifierOnThisServer(), resource.GetAddress(), cluster,
			oldShardInfo.Status, shardInfo.Status)
	}
}

func (l *ClusterEventLogger) OnShardRemoveEvent(cluster *topology.Cluster, resource *pb.StoreResource, shardInfo *pb.ShardInfo) {
	log.Printf("- dc %s  %s on %s cluster %s", resource.DataCenter,
		shardInfo.IdentifierOnThisServer(), resource.Address, cluster)
}

func (l *ClusterEventLogger) OnShardPromoteEvent(cluster *topology.Cluster, resource *pb.StoreResource, shardInfo *pb.ShardInfo) {
	log.Printf("=> dc %s  %s on %s cluster %s", resource.DataCenter,
		shardInfo.IdentifierOnThisServer(), resource.Address, cluster)
}
