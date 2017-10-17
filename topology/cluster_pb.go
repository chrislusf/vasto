package topology

import (
	"github.com/chrislusf/vasto/pb"
)

func NewNodeFromStore(store *pb.StoreResource) *Node {
	return NewNode(
		int(store.Id),
		store.Address,
	)
}

func ToStores(r *ClusterRing) (stores []*pb.StoreResource) {
	if r == nil {
		return
	}
	for i := 0; i < r.NodeCount(); i++ {
		node := r.GetNode(i)
		address := ""
		if node != nil {
			address = node.GetHost()
		}
		stores = append(
			stores,
			&pb.StoreResource{
				Id:      int32(i),
				Address: address,
			},
		)
	}

	return stores
}
