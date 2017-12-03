package topology

import (
	"bytes"
	"fmt"

	"github.com/dgryski/go-jump"
	"github.com/chrislusf/vasto/pb"
)

type Node interface {
	GetId() int
	GetNetwork() string
	GetAddress() string
	GetAdminAddress() string
	SetShardStatus(shardStatus *pb.ShardStatus) (oldShardStatus *pb.ShardStatus)
	RemoveShardStatus(shardStatus *pb.ShardStatus)
	GetShardStatuses() []*pb.ShardStatus
}

type node struct {
	id           int
	network      string
	address      string
	adminAddress string
	shards       map[string]*pb.ShardStatus
}

func (n *node) GetId() int {
	return n.id
}

func (n *node) GetNetwork() string {
	return n.network
}

func (n *node) GetAddress() string {
	return n.address
}

func (n *node) GetAdminAddress() string {
	return n.adminAddress
}

func NewNode(id int, network, address, adminAddress string) Node {
	return &node{id: id, network: network, address: address, adminAddress: adminAddress, shards: make(map[string]*pb.ShardStatus)}
}

func (n *node) SetShardStatus(shardStatus *pb.ShardStatus) (oldShardStatus *pb.ShardStatus) {
	oldShardStatus = n.shards[shardStatus.IdentifierOnThisServer()]
	n.shards[shardStatus.IdentifierOnThisServer()] = shardStatus
	return
}

func (n *node) RemoveShardStatus(shardStatus *pb.ShardStatus) {
	delete(n.shards, shardStatus.IdentifierOnThisServer())
}

func (n *node) GetShardStatuses() []*pb.ShardStatus {
	var statuses []*pb.ShardStatus
	for _, shard := range n.shards {
		ss := shard
		statuses = append(statuses, ss)
	}
	return statuses
}

// --------------------
//      Hash FixedCluster
// --------------------

type ClusterRing struct {
	keyspace     string
	dataCenter   string
	nodes        []Node
	expectedSize int
	nextSize     int
}

// adds a address (+virtual hosts to the ring)
func (cluster *ClusterRing) Add(n Node) {
	if len(cluster.nodes) < n.GetId()+1 {
		cap := n.GetId() + 1
		nodes := make([]Node, cap)
		copy(nodes, cluster.nodes)
		cluster.nodes = nodes
	}
	cluster.nodes[n.GetId()] = n
}

func (cluster *ClusterRing) Remove(nodeId int) Node {
	if nodeId < len(cluster.nodes) {
		n := cluster.nodes[nodeId]
		cluster.nodes[nodeId] = nil
		return n
	}
	return nil
}

// calculates a Jump hash for the key provided
func (cluster *ClusterRing) FindBucketGivenSize(key uint64, size int) int {
	return int(jump.Hash(key, size))
}

// calculates a Jump hash for the key provided
func (cluster *ClusterRing) FindBucket(key uint64) int {
	return int(jump.Hash(key, cluster.ExpectedSize()))
}

func (cluster *ClusterRing) ExpectedSize() int {
	return cluster.expectedSize
}

func (cluster *ClusterRing) NextSize() int {
	return cluster.nextSize
}

func (cluster *ClusterRing) SetExpectedSize(expectedSize int) {
	cluster.expectedSize = expectedSize
}

func (cluster *ClusterRing) SetNextSize(nextSize int) {
	cluster.nextSize = nextSize
}

func (cluster *ClusterRing) CurrentSize() int {
	for i := len(cluster.nodes); i > 0; i-- {
		if cluster.nodes[i-1] == nil || cluster.nodes[i-1].GetAddress() == "" {
			continue
		}
		return i
	}
	return 0
}

func (cluster *ClusterRing) GetNode(index int, options ...AccessOption) (Node, int, bool) {
	replica := 0
	for _, option := range options {
		index, replica = option(index)
	}
	if index < 0 || index >= len(cluster.nodes) {
		return nil, 0, false
	}
	if cluster.nodes[index] == nil {
		return nil, 0, false
	}
	return cluster.nodes[index], replica, true
}

func (cluster *ClusterRing) MissingAndFreeNodeIds() (missingList, freeList []int) {
	max := len(cluster.nodes)
	currentClusterSize := cluster.CurrentSize()
	if max < currentClusterSize {
		max = currentClusterSize
	}
	for i := 0; i < max; i++ {
		var n Node
		if i < len(cluster.nodes) {
			n = cluster.nodes[i]
		}
		if n == nil || n.GetAddress() == "" {
			if i < currentClusterSize {
				missingList = append(missingList, i)
			}
		} else {
			if i >= currentClusterSize {
				freeList = append(freeList, i)
			}
		}
	}
	return
}

// NewHashRing creates a new hash ring.
func NewHashRing(keyspace, dataCenter string, expectedSize int) *ClusterRing {
	return &ClusterRing{
		keyspace:     keyspace,
		dataCenter:   dataCenter,
		nodes:        make([]Node, 0, 16),
		expectedSize: expectedSize,
	}
}

func (cluster *ClusterRing) String() string {
	var output bytes.Buffer
	output.Write([]byte{'['})
	nodeCount := cluster.CurrentSize()
	max := len(cluster.nodes)
	if max < cluster.expectedSize {
		max = cluster.expectedSize
	}
	for i := 0; i < max; i++ {
		var n Node
		if i < len(cluster.nodes) {
			n = cluster.nodes[i]
		}
		if n == nil || n.GetAddress() == "" {
			if i < cluster.expectedSize || i < nodeCount {
				if i != 0 {
					output.Write([]byte{' '})
				}
				output.Write([]byte{'_'})
			}
		} else {
			if i != 0 {
				output.Write([]byte{' '})
			}
			output.WriteString(fmt.Sprintf("%d", n.GetId()))
		}
	}
	output.Write([]byte{']'})
	if cluster.nextSize == 0 {
		if cluster.CurrentSize() != cluster.expectedSize && cluster.expectedSize != 0 {
			output.WriteString(fmt.Sprintf(" size %d->%d ", cluster.CurrentSize(), cluster.expectedSize))
		} else {
			output.WriteString(fmt.Sprintf(" size %d ", cluster.CurrentSize()))
		}
	} else {
		output.WriteString(fmt.Sprintf(" size %d=>%d ", cluster.CurrentSize(), cluster.nextSize))
	}

	missingList, freeList := cluster.MissingAndFreeNodeIds()

	if len(missingList) > 0 || len(freeList) > 0 {
		output.Write([]byte{'('})
		if len(missingList) > 0 {
			output.WriteString(fmt.Sprintf("%d missing %v", len(missingList), missingList))
		}
		if len(freeList) > 0 {
			if len(missingList) > 0 {
				output.Write([]byte{',', ' '})
			}
			output.WriteString(fmt.Sprintf("%d free %v", len(freeList), freeList))
		}
		output.Write([]byte{')'})
	}
	return output.String()
}
