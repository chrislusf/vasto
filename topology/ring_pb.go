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

func ToStores(r Ring) (stores []*pb.StoreResource) {
	if r == nil {
		return
	}
	for i := 0; i < r.Size(); i++ {
		node := r.GetNode(i)
		address := ""
		if node != nil {
			address = node.GetHost()
		}
		stores = append(
			stores,
			&pb.StoreResource{
				Id: int32(i),
				Location: &pb.Location{
					DataCenter: r.GetDataCenter(),
					Address:    address,
				},
			},
		)
	}

	return stores
}
