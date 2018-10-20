package topology

import (
	"fmt"
	"github.com/chrislusf/vasto/pb"
	"github.com/magiconair/properties/assert"
	"testing"
)

func TestClusterOperations(t *testing.T) {
	ring0 := createRing(0)
	assert.Equal(t, ring0.String(), "[] size 0/0 ", "ring 0 to string")

	ring3 := createRing(3)
	assert.Equal(t, ring3.String(), "[0@0,1 1@1,2 2@2,0] size 3/3 ", "ring 3 to string")

	ring3.Debug("test ")

	node, found := ring3.GetNode(1, 1)

	assert.Equal(t, found, true, "found node")
	assert.Equal(t, node.ShardInfo.ShardId, uint32(1), "shard id")
	assert.Equal(t, ring3.ExpectedSize(), 3, "expected cluster size")
	assert.Equal(t, ring3.ReplicationFactor(), 2, "expected ReplicationFactor")

}

func TestClusterProto(t *testing.T) {

	ring3 := createRing(3)

	cluster := ring3.ToCluster()

	assert.Equal(t, cluster.Keyspace, "ks1", "keyspace")
	assert.Equal(t, cluster.ExpectedClusterSize, uint32(3), "expected cluster size")
	assert.Equal(t, cluster.CurrentClusterSize, uint32(3), "current cluster size")

}

func TestReplaceShard(t *testing.T) {

	ring3 := createRing(3)

	x := 5

	node, _ := ring3.GetNode(1, 0)
	assert.Equal(t, node.StoreResource.Address, "localhost:7001", "original server address")

	store := &pb.StoreResource{
		Network:      "tcp",
		Address:      fmt.Sprint("localhost:", 7000+x),
		AdminAddress: fmt.Sprint("localhost:", 8000+x),
	}

	shard := &pb.ShardInfo{
		KeyspaceName:      "ks1",
		ServerId:          uint32(1),
		ShardId:           uint32(1),
		ClusterSize:       uint32(3),
		ReplicationFactor: uint32(2),
	}

	isReplaced := ring3.ReplaceShard(store, shard)

	assert.Equal(t, isReplaced, true, "replace shard")

	node, _ = ring3.GetNode(1, 0)

	assert.Equal(t, node.StoreResource.Address, store.Address, "replaced server address")

}

func TestRemoveShard(t *testing.T) {

	ring3 := createRing(3)

	x := 1

	node, _ := ring3.GetNode(1, 0)
	assert.Equal(t, node.StoreResource.Address, "localhost:7001", "original server address")

	store := &pb.StoreResource{
		Network:      "tcp",
		Address:      fmt.Sprint("localhost:", 7000+x),
		AdminAddress: fmt.Sprint("localhost:", 8000+x),
	}

	shard0 := &pb.ShardInfo{
		KeyspaceName:      "ks1",
		ServerId:          uint32(1),
		ShardId:           uint32(0),
		ClusterSize:       uint32(3),
		ReplicationFactor: uint32(2),
	}

	shard1 := &pb.ShardInfo{
		KeyspaceName:      "ks1",
		ServerId:          uint32(1),
		ShardId:           uint32(1),
		ClusterSize:       uint32(3),
		ReplicationFactor: uint32(2),
	}

	isStoreDeleted := ring3.RemoveShard(store, shard1)

	assert.Equal(t, isStoreDeleted, false, "remove shard 1")

	isStoreDeleted = ring3.RemoveShard(store, shard0)

	assert.Equal(t, isStoreDeleted, true, "remove shard 0")

}

func TestRemoveStore(t *testing.T) {

	ring3 := createRing(3)

	x := 1

	node, _ := ring3.GetNode(1, 0)
	assert.Equal(t, node.StoreResource.Address, "localhost:7001", "original server address")

	store := &pb.StoreResource{
		Network:      "tcp",
		Address:      fmt.Sprint("localhost:", 7000+x),
		AdminAddress: fmt.Sprint("localhost:", 8000+x),
	}

	shard0 := &pb.ShardInfo{
		KeyspaceName:      "ks1",
		ServerId:          uint32(1),
		ShardId:           uint32(0),
		ClusterSize:       uint32(3),
		ReplicationFactor: uint32(2),
	}

	shard1 := &pb.ShardInfo{
		KeyspaceName:      "ks1",
		ServerId:          uint32(1),
		ShardId:           uint32(1),
		ClusterSize:       uint32(3),
		ReplicationFactor: uint32(2),
	}

	removedShards := ring3.RemoveStore(store)

	assert.Equal(t, len(removedShards), 2, "remove shard count")

	assert.Equal(t, removedShards[0].ShardId, shard0.ShardId, "remove shard 0")
	assert.Equal(t, removedShards[1].ShardId, shard1.ShardId, "remove shard 1")

}

func TestNextCluster(t *testing.T) {

	ring := createRing(0)

	x := 1

	store := &pb.StoreResource{
		Network:      "tcp",
		Address:      fmt.Sprint("localhost:", 7000+x),
		AdminAddress: fmt.Sprint("localhost:", 8000+x),
	}

	shard0 := &pb.ShardInfo{
		KeyspaceName:      "ks1",
		ServerId:          uint32(0),
		ShardId:           uint32(0),
		ClusterSize:       uint32(1),
		ReplicationFactor: uint32(1),
	}

	ring.SetShard(store, shard0)
	assert.Equal(t, ring.ExpectedSize(), 1, "add shard 0")
	assert.Equal(t, ring.CurrentSize(), 1, "current cluster size")

	ring.SetNextCluster(2, 1)

	ring.RemoveNextCluster()

}
