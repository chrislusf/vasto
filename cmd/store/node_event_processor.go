package store

import (
	"github.com/chrislusf/vasto/topology"
	"github.com/chrislusf/vasto/pb"
	"log"
)

func (n *node) startListenForNodePeerEvents() {

	n.clusterListener.RegisterShardEventProcessor(n)

	select {
	case <-n.clusterListenerFinishChan:
		n.clusterListener.UnregisterShardEventProcessor(n)
	}

}

func (n *node) stopListenForNodePeerEvents() {

	close(n.clusterListenerFinishChan)

}

// the following functions implements cluster_listener.ShardEventProcessor

func (n *node) OnShardCreateEvent(cluster *topology.ClusterRing, resource *pb.StoreResource, shardStatus *pb.ShardStatus) {
	log.Printf("+ keyspace %s node %d shard %d cluster %s",
		shardStatus.KeyspaceName, shardStatus.NodeId, shardStatus.ShardId, cluster)
	if n.keyspace != shardStatus.KeyspaceName {
		return
	}
	if n.id == int(shardStatus.ShardId) {
		return
	}
	log.Printf("+ peer keyspace %s shard %d found peer shard %d",
		shardStatus.KeyspaceName, n.id, shardStatus.ShardId)
}

func (n *node) OnShardUpdateEvent(cluster *topology.ClusterRing, resource *pb.StoreResource, shardStatus *pb.ShardStatus, oldShardStatus *pb.ShardStatus) {
	if oldShardStatus == nil {
	} else if oldShardStatus.Status != shardStatus.Status {
		log.Printf("+ keyspace %s node %d shard %d cluster %s status:%s=>%s",
			shardStatus.KeyspaceName, shardStatus.NodeId, shardStatus.ShardId, cluster, oldShardStatus.Status, shardStatus.Status)
	}
}
func (n *node) OnShardRemoveEvent(cluster *topology.ClusterRing, resource *pb.StoreResource, shardStatus *pb.ShardStatus) {
	log.Printf("- keyspace %s node %d shard %d cluster %s",
		shardStatus.KeyspaceName, shardStatus.NodeId, shardStatus.ShardId, cluster)
	log.Printf("- dc %s keyspace %s node %d shard %d %s cluster %s", resource.DataCenter,
		shardStatus.KeyspaceName, shardStatus.NodeId, shardStatus.ShardId, resource.Address, cluster)
}
