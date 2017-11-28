package topology

import (
	"github.com/chrislusf/vasto/pb"
)

func NewNodeFromStore(store *pb.StoreResource, shardId uint32) Node {
	return NewNode(
		int(shardId),
		store.Network,
		store.Address,
		store.AdminAddress,
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

func (r *ClusterRing) toNodes() (stores []*pb.ClusterNode) {
	if r == nil {
		return
	}
	for i := 0; i < r.CurrentSize(); i++ {
		node, _, ok := r.GetNode(i)
		if !ok {
			continue
		}
		var network, address, adminAddress string
		if node != nil {
			network = node.GetNetwork()
			address = node.GetAddress()
			adminAddress = node.GetAdminAddress()
		}
		stores = append(
			stores,
			&pb.ClusterNode{
				ShardId:      uint32(i),
				Network:      network,
				Address:      address,
				AdminAddress: adminAddress,
			},
		)
	}

	return stores
}
