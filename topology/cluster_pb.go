package topology

import (
	"github.com/chrislusf/vasto/pb"
)

func NewNodeFromStore(store *pb.StoreResource, shardId uint32) Node {
	return NewNode(
		int(shardId),
		store,
	)
}

func (cluster *ClusterRing) ToCluster() *pb.Cluster {
	if cluster == nil {
		return &pb.Cluster{}
	}
	return &pb.Cluster{
		Keyspace:            cluster.keyspace,
		DataCenter:          cluster.dataCenter,
		Nodes:               cluster.toNodes(),
		ExpectedClusterSize: uint32(cluster.ExpectedSize()),
		CurrentClusterSize:  uint32(cluster.CurrentSize()),
		NextClusterSize:     uint32(cluster.NextSize()),
	}
}

func (cluster *ClusterRing) toNodes() (nodes []*pb.ClusterNode) {
	if cluster == nil {
		return
	}
	for i := 0; i < cluster.CurrentSize(); i++ {
		node, _, ok := cluster.GetNode(i)
		if !ok {
			continue
		}
		var network, address, adminAddress string
		if node != nil {
			network = node.GetNetwork()
			address = node.GetAddress()
			adminAddress = node.GetAdminAddress()
		}
		for _, shardStatus := range node.GetShardStatuses() {
			ss := shardStatus
			nodes = append(
				nodes,
				&pb.ClusterNode{
					StoreResource: &pb.StoreResource{
						DataCenter:   cluster.dataCenter,
						Network:      network,
						Address:      address,
						AdminAddress: adminAddress,
					},
					ShardStatus: ss,
				},
			)
		}
	}

	return nodes
}
