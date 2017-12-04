package cluster_listener

import (
	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/topology"
)

type ShardEventProcessor interface {
	OnShardCreateEvent(cluster *topology.ClusterRing, resource *pb.StoreResource, shardStatus *pb.ShardStatus)
	OnShardUpdateEvent(cluster *topology.ClusterRing, resource *pb.StoreResource, shardStatus *pb.ShardStatus, oldShardStatus *pb.ShardStatus)
	OnShardRemoveEvent(cluster *topology.ClusterRing, resource *pb.StoreResource, shardStatus *pb.ShardStatus)
	// OnShardResizeEvent(resource *pb.StoreResource, status *pb.ShardStatus)
}

func (clusterListener *ClusterListener) RegisterShardEventProcessor(shardEventProcess ShardEventProcessor) {
	clusterListener.shardEventProcessors = append(clusterListener.shardEventProcessors, shardEventProcess)
}
