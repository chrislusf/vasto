package cluster_listener

import (
	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/topology"
)

func AddNode(cluster *topology.Cluster, n *pb.ClusterNode) (oldShardInfo *pb.ShardInfo) {

	if n.ShardInfo.IsCandidate {
		if cluster.GetNextCluster() == nil {
			cluster.SetNextCluster(int(n.ShardInfo.ClusterSize), int(n.ShardInfo.ReplicationFactor))
		}
		cluster = cluster.GetNextCluster()
	}

	return cluster.SetShard(n.StoreResource, n.ShardInfo)
}

func (clusterListener *ClusterListener) RemoveNode(keyspace string, n *pb.ClusterNode) {
	cluster, found := clusterListener.GetCluster(keyspace)
	if !found {
		return
	}
	if n.ShardInfo.IsCandidate {
		if cluster.GetNextCluster() == nil {
			cluster.SetNextCluster(int(n.ShardInfo.ClusterSize), int(n.ShardInfo.ReplicationFactor))
		}
		cluster = cluster.GetNextCluster()
	}

	isStoreDeleted := cluster.RemoveShard(n.StoreResource, n.ShardInfo)
	if isStoreDeleted {
		clusterListener.connPoolLock.Lock()
		connPool, foundPool := clusterListener.connPools[n.StoreResource.Address]
		if foundPool {
			delete(clusterListener.connPools, n.StoreResource.Address)
			connPool.Close()
		}
		clusterListener.connPoolLock.Unlock()
	}
}

func PromoteNode(cluster *topology.Cluster, n *pb.ClusterNode) {

	candidateCluster := cluster.GetNextCluster()

	if candidateCluster == nil {
		return
	}

	if n != nil {
		candidateCluster.RemoveShard(n.StoreResource, n.ShardInfo)
		if candidateCluster.CurrentSize() == 0 {
			cluster.RemoveNextCluster()
		}
		oldPrimaryNode, _, found := cluster.GetNode(int(n.ShardInfo.ShardId))
		if !found {
			cluster.SetShard(n.StoreResource, n.ShardInfo)
		} else {
			oldStore := oldPrimaryNode.StoreResource
			cluster.ReplaceShard(oldStore, n.StoreResource, n.ShardInfo)
		}
	}
}
