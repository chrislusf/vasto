package topology

import (
	"github.com/chrislusf/vasto/pb"
)

func NewNodeFromStore(store *pb.StoreResource) Node {
	return NewNode(
		int(store.Id),
		store.Location.Address,
	)
}

func ToStores(ring Ring) (stores []*pb.StoreResource) {
	for i := 0; i < ring.Size(); i++ {
		node := ring.GetNode(i)
		if node == nil {
			continue
		}
		stores = append(stores, &pb.StoreResource{
			Id: int32(i),
			Location: &pb.Location{
				DataCenter: ring.GetDataCenter(),
				Address:    node.GetHost(),
			},
		})
	}
	return stores
}
