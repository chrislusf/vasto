package topology

import (
	"testing"
	"github.com/magiconair/properties/assert"
	"github.com/chrislusf/vasto/pb"
	"google.golang.org/grpc"
)

func TestClusterWithConnection(t *testing.T) {
	ring0 := createRing(0)

	err := ring0.WithConnection("failed test", 2, nil)
	assert.Equal(t, err != nil, true, "ring 0 with connection to server 2")

	ring3 := createRing(3)
	err = ring3.WithConnection("failed test", 2, func(node *pb.ClusterNode, conn *grpc.ClientConn) error {

		assert.Equal(t, node.StoreResource.Address, "localhost:7002", "ring 3 with connection to server 2")

		return nil
	})
	assert.Equal(t, err, nil, "ring 0 with connection to server 2")

}
