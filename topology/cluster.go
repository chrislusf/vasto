package topology

import (
	"bytes"
	"fmt"

	"github.com/chrislusf/glog"
	"github.com/chrislusf/vasto/pb"
	"github.com/dgryski/go-jump"
	"sort"
)

// Cluster manages one cluster topology
type Cluster struct {
	keyspace          string
	dataCenter        string
	logicalShards     []LogicalShardGroup
	expectedSize      int
	replicationFactor int
	nextCluster       *Cluster
}

// LogicalShardGroup is a list of shards with the same shard id
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

// SetShard sets the tuple of server and shardInfo to the cluster.
// It returns the previous shardInfo if found.
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
	if cluster.expectedSize != int(shard.ClusterSize) {
		cluster.SetExpectedSize(int(shard.ClusterSize))
	}
	if cluster.replicationFactor != int(shard.ReplicationFactor) {
		cluster.SetReplicationFactor(int(shard.ReplicationFactor))
	}
	return
}

// ReplaceShard ReplaceShard the shardInfo on the server in the cluster.
// It returns true if the operation is successful.
func (cluster *Cluster) ReplaceShard(newStore *pb.StoreResource, shard *pb.ShardInfo) (isReplaced bool) {
	shardId := int(shard.ShardId)
	if len(cluster.logicalShards) < shardId+1 {
		capacity := shardId + 1
		nodes := make([]LogicalShardGroup, capacity)
		copy(nodes, cluster.logicalShards)
		cluster.logicalShards = nodes
	}
	shardGroup := cluster.logicalShards[shardId]
	for i := 0; i < len(shardGroup); i++ {
		if shardGroup[i].ShardInfo.IdentifierOnThisServer() == shard.IdentifierOnThisServer() {
			shardGroup[i].ShardInfo = shard
			shardGroup[i].StoreResource = newStore
			return true
		}
	}
	glog.Errorf("replace shard error: shard %s not found", shard.IdentifierOnThisServer())
	return false
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

	// if no shards and no clients, set the cluster size to be 0
	if cluster.CurrentSize() == 0 {
		cluster.expectedSize = 0
		cluster.logicalShards = nil
	}

	// check other shards that may be using the store
	for _, shardGroup := range cluster.logicalShards {
		for i := 0; i < len(shardGroup); i++ {
			if shardGroup[i] != nil && shardGroup[i].StoreResource.Address == store.Address {
				return false
			}
		}
	}

	return !cluster.isStoreInUse(store) && !cluster.GetNextCluster().isStoreInUse(store)
}

// RemoveStore removes the server from the cluster.
// It returns the shards which were on the server.
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
			if shardGroup[i] != nil && shardGroup[i].StoreResource.Address == store.Address {
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

// FindShardId calculates a Jump hash for the keyHash provided
func (cluster *Cluster) FindShardId(keyHash uint64) int {
	return int(jump.Hash(keyHash, cluster.expectedSize))
}

// ExpectedSize returns the expected size of the cluster
func (cluster *Cluster) ExpectedSize() int {
	return cluster.expectedSize
}

// ReplicationFactor returns the replication factor of the cluster
func (cluster *Cluster) ReplicationFactor() int {
	return cluster.replicationFactor
}

// SetExpectedSize sets the expected size of the cluster
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

// SetNextCluster creates a new cluster and sets the size and replication factor
func (cluster *Cluster) SetNextCluster(expectedSize int, replicationFactor int) *Cluster {
	cluster.nextCluster = NewCluster(cluster.keyspace, cluster.dataCenter, expectedSize, replicationFactor)
	return cluster.nextCluster
}

// GetNextCluster returns the next cluster
func (cluster *Cluster) GetNextCluster() *Cluster {
	return cluster.nextCluster
}

// RemoveNextCluster clears the pointer to the next cluster
func (cluster *Cluster) RemoveNextCluster() {
	cluster.nextCluster = nil
}

// SetReplicationFactor sets the replication factor of the cluster
func (cluster *Cluster) SetReplicationFactor(replicationFactor int) {
	if replicationFactor > 0 {
		cluster.replicationFactor = replicationFactor
	}
}

// CurrentSize returns the cluster current size
func (cluster *Cluster) CurrentSize() int {
	for i := len(cluster.logicalShards); i > 0; i-- {
		if len(cluster.logicalShards[i-1]) == 0 {
			continue
		}
		return i
	}
	return 0
}

// GetNode returns the server having the shard.
// replica denotes the shard replica.
func (cluster *Cluster) GetNode(shardId int, replica int) (*pb.ClusterNode, bool) {
	shards := cluster.getShards(shardId)
	if replica >= len(shards) {
		return nil, false
	}

	return shards[replica], true
}

func (cluster *Cluster) getShards(shardId int) LogicalShardGroup {
	if shardId < 0 || shardId >= len(cluster.logicalShards) {
		return nil
	}
	return cluster.logicalShards[shardId]
}

// GetAllShards returns a list of all logic shard groups.
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

// Debug prints out the detailed info of the cluster.
func (cluster *Cluster) Debug(prefix string) {
	for _, shardGroup := range cluster.GetAllShards() {
		for _, shard := range shardGroup {
			fmt.Printf("%s* %+v on %v usage:%d/%d\n", prefix, shard.ShardInfo.IdentifierOnThisServer(), shard.StoreResource.Address, shard.StoreResource.AllocatedSizeGb, shard.StoreResource.DiskSizeGb)
		}
	}

	if cluster.nextCluster != nil {
		cluster.nextCluster.Debug(prefix + "  >")
	}

}
