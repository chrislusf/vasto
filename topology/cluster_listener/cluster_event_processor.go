package cluster_listener

import (
	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/topology"
	"log"
)

type ShardEventProcessor interface {
	OnShardCreateEvent(cluster *topology.Cluster, resource *pb.StoreResource, ShardInfo *pb.ShardInfo)
	OnShardUpdateEvent(cluster *topology.Cluster, resource *pb.StoreResource, ShardInfo *pb.ShardInfo, oldShardInfo *pb.ShardInfo)
	OnShardRemoveEvent(cluster *topology.Cluster, resource *pb.StoreResource, ShardInfo *pb.ShardInfo)
	OnShardPromoteEvent(cluster *topology.Cluster, resource *pb.StoreResource, ShardInfo *pb.ShardInfo)
	// OnShardResizeEvent(resource *pb.StoreResource, status *pb.ShardInfo)
}

func (clusterListener *ClusterListener) RegisterShardEventProcessor(shardEventProcess ShardEventProcessor) {
	found := -1
	for k, p := range clusterListener.shardEventProcessors {
		if p == shardEventProcess {
			found = k
		}
	}
	if found >= 0 {
		return
	}
	clusterListener.shardEventProcessors = append(clusterListener.shardEventProcessors, shardEventProcess)
}

func (clusterListener *ClusterListener) UnregisterShardEventProcessor(shardEventProcess ShardEventProcessor) {
	found := -1
	for k, p := range clusterListener.shardEventProcessors {
		if p == shardEventProcess {
			found = k
		}
	}
	if found < 0 {
		log.Printf("removing failed! %+v", shardEventProcess)
		return
	}
	copy(clusterListener.shardEventProcessors[found:], clusterListener.shardEventProcessors[found+1:])
	clusterListener.shardEventProcessors[len(clusterListener.shardEventProcessors)-1] = nil // or the zero value of T
	clusterListener.shardEventProcessors = clusterListener.shardEventProcessors[:len(clusterListener.shardEventProcessors)-1]
}
