package topology

import (
	"testing"
	"github.com/magiconair/properties/assert"
)

func TestClusterOperations(t *testing.T) {
	ring0 := createRing(0)
	assert.Equal(t, ring0.String(), "[] size 0/0 ", "ring 0 to string")

	ring3 := createRing(3)
	assert.Equal(t, ring3.String(), "[0@0,1 1@1,2 2@2,0] size 3/3 ", "ring 3 to string")

	ring3.Debug("test ")

	node, replica, found := ring3.GetNode(1, NewAccessOption(1))

	assert.Equal(t, found, true, "found node")
	assert.Equal(t, replica, 1, "replica")
	assert.Equal(t, node.ShardInfo.ShardId, 1, "shard id")
	assert.Equal(t, ring3.ExpectedSize(), 3, "expected cluster size")
	assert.Equal(t, ring3.ReplicationFactor(), 2, "expected ReplicationFactor")

}

func TestClusterProto(t *testing.T) {

	ring3 := createRing(3)

	cluster := ring3.ToCluster()

	assert.Equal(t, cluster.Keyspace, "ks1", "keyspace")
	assert.Equal(t, cluster.DataCenter, "dc1", "data center")
	assert.Equal(t, cluster.ExpectedClusterSize, uint32(3), "expected cluster size")
	assert.Equal(t, cluster.CurrentClusterSize, uint32(3), "current cluster size")

}
