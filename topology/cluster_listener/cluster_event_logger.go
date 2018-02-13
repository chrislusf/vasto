package cluster_listener

import (
	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/topology"
	"github.com/golang/glog"
)

type ClusterEventLogger struct {
	Prefix string
}

func (l *ClusterEventLogger) OnShardCreateEvent(cluster *topology.Cluster, resource *pb.StoreResource, shardInfo *pb.ShardInfo) {
	glog.V(1).Infof("%s+ dc %s keyspace %s node %d shard %d %s cluster %s", l.Prefix, resource.DataCenter,
		shardInfo.KeyspaceName, shardInfo.ServerId, shardInfo.ShardId, resource.Address, cluster)
}

func (l *ClusterEventLogger) OnShardUpdateEvent(cluster *topology.Cluster, resource *pb.StoreResource, shardInfo *pb.ShardInfo, oldShardInfo *pb.ShardInfo) {
	if oldShardInfo == nil {
	} else if oldShardInfo.Status != shardInfo.Status {
		glog.V(1).Infof("%s* dc %s %s on %s cluster %s status:%s=>%s", l.Prefix, resource.DataCenter,
			shardInfo.IdentifierOnThisServer(), resource.GetAddress(), cluster,
			oldShardInfo.Status, shardInfo.Status)
	}
}

func (l *ClusterEventLogger) OnShardRemoveEvent(cluster *topology.Cluster, resource *pb.StoreResource, shardInfo *pb.ShardInfo) {
	glog.V(1).Infof("%s- dc %s  %s on %s cluster %s", l.Prefix, resource.DataCenter,
		shardInfo.IdentifierOnThisServer(), resource.Address, cluster)
}

func (l *ClusterEventLogger) OnShardPromoteEvent(cluster *topology.Cluster, resource *pb.StoreResource, shardInfo *pb.ShardInfo) {
	glog.V(1).Infof("%s=> dc %s  %s on %s cluster %s", l.Prefix, resource.DataCenter,
		shardInfo.IdentifierOnThisServer(), resource.Address, cluster)
}
