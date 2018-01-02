package topology

import (
	"github.com/chrislusf/vasto/pb"
)

func (cluster *Cluster) ToCluster() *pb.Cluster {
	if cluster == nil {
		return &pb.Cluster{}
	}
	return &pb.Cluster{
		Keyspace:            cluster.keyspace,
		DataCenter:          cluster.dataCenter,
		Nodes:               cluster.toNodes(),
		ExpectedClusterSize: uint32(cluster.ExpectedSize()),
		CurrentClusterSize:  uint32(cluster.CurrentSize()),
	}
}

func (cluster *Cluster) toNodes() (nodes []*pb.ClusterNode) {
	if cluster == nil {
		return
	}
	for _, shards := range cluster.logicalShards {
		for _, shard := range shards {
			nodes = append(
				nodes,
				&pb.ClusterNode{
					StoreResource: shard.StoreResource,
					ShardInfo:     shard.ShardInfo,
				},
			)
		}
	}

	return nodes
}
