package clusterlistener

import (
	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/topology"
)

func addNode(cluster *topology.Cluster, n *pb.ClusterNode) (oldShardInfo *pb.ShardInfo) {

	if n.ShardInfo.IsCandidate {
		if cluster.GetNextCluster() == nil {
			cluster.SetNextCluster(int(n.ShardInfo.ClusterSize), int(n.ShardInfo.ReplicationFactor))
		}
		cluster = cluster.GetNextCluster()
	}

	return cluster.SetShard(n.StoreResource, n.ShardInfo)
}

func (clusterListener *ClusterListener) removeNode(keyspace string, n *pb.ClusterNode) {
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

func promoteNode(cluster *topology.Cluster, n *pb.ClusterNode) {

	candidateCluster := cluster.GetNextCluster()

	if candidateCluster == nil {
		return
	}

	if n != nil {
		candidateCluster.RemoveShard(n.StoreResource, n.ShardInfo)
		if candidateCluster.CurrentSize() == 0 {
			cluster.RemoveNextCluster()
		}
		if !cluster.ReplaceShard(n.StoreResource, n.ShardInfo) {
			cluster.SetShard(n.StoreResource, n.ShardInfo)
		}
	}
}
