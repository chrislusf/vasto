package topology

import (
	"bytes"
	"fmt"

	"github.com/dgryski/go-jump"
	"github.com/chrislusf/vasto/pb"
	"sort"
)

// --------------------
//      Hash FixedCluster
// --------------------

type Cluster struct {
	keyspace          string
	dataCenter        string
	logicalShards     []LogicalShardGroup
	expectedSize      int
	replicationFactor int
	nextCluster       *Cluster
}

type LogicalShardGroup []*pb.ClusterNode

func (shards LogicalShardGroup) String() string {
	if len(shards) == 0 {
		return ""
	}
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("%d@", shards[0].ShardInfo.ShardId))
	for i, shard := range shards {
		if i != 0 {
			buf.WriteString(",")
		}
		buf.WriteString(fmt.Sprintf("%d", shard.ShardInfo.ServerId))
	}
	return buf.String()
}

func (cluster *Cluster) SetShard(store *pb.StoreResource, shard *pb.ShardInfo) (oldShardInfo *pb.ShardInfo) {
	shardId := int(shard.ShardId)
	if len(cluster.logicalShards) < shardId+1 {
		capacity := shardId + 1
		nodes := make([]LogicalShardGroup, capacity)
		copy(nodes, cluster.logicalShards)
		cluster.logicalShards = nodes
	}
	shardGroup := cluster.logicalShards[shardId]
	for i := 0; i < len(shardGroup); i++ {
		if shardGroup[i].StoreResource.Address == store.Address && shardGroup[i].ShardInfo.ShardId == shard.ShardId {
			oldShardInfo = shardGroup[i].ShardInfo
			shardGroup[i].ShardInfo = shard
			return
		}
	}
	shardGroup = append(shardGroup, &pb.ClusterNode{
		StoreResource: store,
		ShardInfo:     shard,
	})
	cluster.logicalShards[shardId] = sortedShards(shardGroup, len(cluster.logicalShards))
	return
}

// RemoveShard returns true if no other shards is on this store
func (cluster *Cluster) RemoveShard(store *pb.StoreResource, shard *pb.ShardInfo) (storeDeleted bool) {
	shardId := int(shard.ShardId)
	if len(cluster.logicalShards) <= shardId {
		return
	}
	shardGroup := cluster.logicalShards[shardId]
	for i := 0; i < len(shardGroup); i++ {
		if shardGroup[i].StoreResource.Address == store.Address && shardGroup[i].ShardInfo.ShardId == shard.ShardId {
			copy(shardGroup[i:], shardGroup[i+1:])
			shardGroup[len(shardGroup)-1] = nil // or the zero value of T
			shardGroup = shardGroup[:len(shardGroup)-1]
			cluster.logicalShards[shardId] = sortedShards(shardGroup, len(cluster.logicalShards))
			break
		}
	}

	// check other shards that may be using the store
	for _, shardGroup := range cluster.logicalShards {
		for i := 0; i < len(shardGroup); i++ {
			if shardGroup[i].StoreResource.Address == store.Address {
				return false
			}
		}
	}

	// if no shards and no clients, set the cluster size to be 0
	if cluster.CurrentSize() == 0 {
		cluster.expectedSize = 0
		cluster.logicalShards = nil
	}

	return !cluster.isStoreInUse(store) && !cluster.GetNextCluster().isStoreInUse(store)
}

func (cluster *Cluster) RemoveStore(store *pb.StoreResource) (removedShards []*pb.ShardInfo) {
	for shardId, shardGroup := range cluster.logicalShards {
		for i := 0; i < len(shardGroup); i++ {
			if shardGroup[i].StoreResource.Address == store.Address {

				removedShards = append(removedShards, shardGroup[i].ShardInfo)

				copy(shardGroup[i:], shardGroup[i+1:])
				shardGroup[len(shardGroup)-1] = nil // or the zero value of T
				shardGroup = shardGroup[:len(shardGroup)-1]
				i--
			}
		}
		cluster.logicalShards[shardId] = sortedShards(shardGroup, len(cluster.logicalShards))
	}
	return
}

func (cluster *Cluster) isStoreInUse(store *pb.StoreResource) bool {
	if cluster == nil {
		return false
	}
	// check other shards that may be using the store
	for _, shardGroup := range cluster.logicalShards {
		for i := 0; i < len(shardGroup); i++ {
			if shardGroup[i].StoreResource.Address == store.Address {
				return true
			}
		}
	}
	return false
}

func sortedShards(shards LogicalShardGroup, clusterSize int) LogicalShardGroup {
	sort.Slice(shards, func(i, j int) bool {
		x := int(shards[i].ShardInfo.ServerId) - int(shards[i].ShardInfo.ShardId)
		if x < 0 {
			x += clusterSize
		}
		y := int(shards[j].ShardInfo.ServerId) - int(shards[j].ShardInfo.ShardId)
		if y < 0 {
			y += clusterSize
		}
		return x < y
	})
	return shards
}

// calculates a Jump hash for the keyHash provided
func (cluster *Cluster) FindShardId(keyHash uint64) int {
	return int(jump.Hash(keyHash, cluster.expectedSize))
}

func (cluster *Cluster) ExpectedSize() int {
	return cluster.expectedSize
}

func (cluster *Cluster) ReplicationFactor() int {
	return cluster.replicationFactor
}

func (cluster *Cluster) SetExpectedSize(expectedSize int) {
	if expectedSize > 0 {
		cluster.expectedSize = expectedSize
		if len(cluster.logicalShards) == 0 {
			cluster.logicalShards = make([]LogicalShardGroup, expectedSize)
		}
		if expectedSize < len(cluster.logicalShards) {
			cluster.logicalShards = cluster.logicalShards[0:expectedSize]
		}
	}
}

func (cluster *Cluster) SetNextCluster(expectedSize int, replicationFactor int) *Cluster {
	cluster.nextCluster = NewCluster(cluster.keyspace, cluster.dataCenter, expectedSize, replicationFactor)
	return cluster.nextCluster
}

func (cluster *Cluster) GetNextCluster() *Cluster {
	return cluster.nextCluster
}

func (cluster *Cluster) RemoveNextCluster() {
	cluster.nextCluster = nil
}

func (cluster *Cluster) SetReplicationFactor(replicationFactor int) {
	if replicationFactor > 0 {
		cluster.replicationFactor = replicationFactor
	}
}

func (cluster *Cluster) CurrentSize() int {
	for i := len(cluster.logicalShards); i > 0; i-- {
		if len(cluster.logicalShards[i-1]) == 0 {
			continue
		}
		return i
	}
	return 0
}

func (cluster *Cluster) GetNode(shardId int, options ...AccessOption) (*pb.ClusterNode, int, bool) {
	replica := 0
	shards := cluster.getShards(shardId)
	for _, option := range options {
		_, replica = option(shardId, cluster.expectedSize)
	}
	if replica < 0 || replica >= len(shards) {
		return nil, 0, false
	}

	return shards[replica], replica, true
}

func (cluster *Cluster) getShards(shardId int) LogicalShardGroup {
	if shardId < 0 || shardId >= len(cluster.logicalShards) {
		return nil
	}
	return cluster.logicalShards[shardId]
}

func (cluster *Cluster) GetAllShards() []LogicalShardGroup {
	return cluster.logicalShards
}

// NewCluster creates a new cluster.
func NewCluster(keyspace, dataCenter string, expectedSize int, replicationFactor int) *Cluster {
	return &Cluster{
		keyspace:          keyspace,
		dataCenter:        dataCenter,
		logicalShards:     make([]LogicalShardGroup, expectedSize),
		expectedSize:      expectedSize,
		replicationFactor: replicationFactor,
	}
}

func (cluster *Cluster) String() string {
	var output bytes.Buffer
	output.Write([]byte{'['})
	for i := 0; i < len(cluster.logicalShards); i++ {
		if i != 0 {
			output.Write([]byte{' '})
		}
		shards := cluster.logicalShards[i]
		if len(shards) == 0 {
			output.Write([]byte{'_'})
		} else {
			output.WriteString(shards.String())
		}
	}
	output.Write([]byte{']'})
	output.WriteString(fmt.Sprintf(" size %d/%d ", cluster.CurrentSize(), cluster.ExpectedSize()))

	return output.String()
}

func (cluster *Cluster) Debug(prefix string) {
	for _, shardGroup := range cluster.logicalShards {
		for _, shard := range shardGroup {
			fmt.Printf("%s* %+v on %v usage:%d/%d\n", prefix, shard.ShardInfo.IdentifierOnThisServer(), shard.StoreResource.Address, shard.StoreResource.AllocatedSizeGb, shard.StoreResource.DiskSizeGb)
		}
	}

	if cluster.nextCluster != nil {
		cluster.nextCluster.Debug(prefix + "  >")
	}

}
