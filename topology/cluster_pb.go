package topology

import (
	"github.com/chrislusf/vasto/pb"
)

func NewNodeFromStore(store *pb.StoreResource) Node {
	return NewNode(
		int(store.Id),
		store.Network,
		store.Address,
	)
}

func (cluster *ClusterRing) ToCluster() *pb.Cluster {
	if cluster == nil {
		return &pb.Cluster{}
	}
	return &pb.Cluster{
		DataCenter:         cluster.GetDataCenter(),
		Stores:             cluster.ToStores(),
		CurrentClusterSize: uint32(cluster.CurrentSize()),
		NextClusterSize:    uint32(cluster.NextSize()),
	}
}

func (r *ClusterRing) ToStores() (stores []*pb.StoreResource) {
	if r == nil {
		return
	}
	for i := 0; i < r.NodeCount(); i++ {
		node := r.GetNode(i)
		address := ""
		if node != nil {
			address = node.GetAddress()
		}
		stores = append(
			stores,
			&pb.StoreResource{
				Id:      int32(i),
				Network: node.GetNetwork(),
				Address: address,
			},
		)
	}

	return stores
}
