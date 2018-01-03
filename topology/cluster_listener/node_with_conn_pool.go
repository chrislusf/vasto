package cluster_listener

import (
	"github.com/chrislusf/vasto/pb"
)

func (clusterListener *ClusterListener) AddNode(keyspace string, n *pb.ClusterNode) (oldShardInfo *pb.ShardInfo) {
	cluster := clusterListener.GetOrSetCluster(keyspace, int(n.ShardInfo.ClusterSize), int(n.ShardInfo.ReplicationFactor))

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

func (clusterListener *ClusterListener) PromoteNode(keyspace string, n *pb.ClusterNode) {
	cluster, foundCluster := clusterListener.GetCluster(keyspace)
	if !foundCluster {
		return
	}

	candidateCluster := cluster.GetNextCluster()

	if candidateCluster == nil {
		return
	}

	if n != nil {
		candidateCluster.RemoveShard(n.StoreResource, n.ShardInfo)
		if candidateCluster.CurrentSize() == 0 {
			cluster.RemoveNextCluster()
		}
		cluster.SetShard(n.StoreResource, n.ShardInfo)
	}
}
