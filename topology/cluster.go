package topology

import (
	"bytes"
	"fmt"

	"github.com/dgryski/go-jump"
)

// --------------------
//      Hash FixedCluster
// --------------------

type ClusterRing struct {
	keyspace          string
	dataCenter        string
	nodes             []Node
	expectedSize      int
	replicationFactor int
	nextClusterRing   *ClusterRing
}

// adds a address (+virtual hosts to the ring)
func (cluster *ClusterRing) SetNode(n Node) {
	if len(cluster.nodes) < n.GetId()+1 {
		capacity := n.GetId() + 1
		nodes := make([]Node, capacity)
		copy(nodes, cluster.nodes)
		cluster.nodes = nodes
	}
	cluster.nodes[n.GetId()] = n
}

func (cluster *ClusterRing) RemoveNode(nodeId int) Node {
	if nodeId < len(cluster.nodes) {
		n := cluster.nodes[nodeId]
		cluster.nodes[nodeId] = nil
		return n
	}
	return nil
}

// calculates a Jump hash for the keyHash provided
func (cluster *ClusterRing) FindShardId(keyHash uint64) int {
	return int(jump.Hash(keyHash, cluster.ExpectedSize()))
}

func (cluster *ClusterRing) ExpectedSize() int {
	return cluster.expectedSize
}

func (cluster *ClusterRing) ReplicationFactor() int {
	return cluster.replicationFactor
}

func (cluster *ClusterRing) SetExpectedSize(expectedSize int) {
	if expectedSize > 0 {
		cluster.expectedSize = expectedSize
		if len(cluster.nodes) == 0 {
			cluster.nodes = make([]Node, expectedSize)
		}
	}
}

func (cluster *ClusterRing) SetNextClusterRing(expectedSize int, replicationFactor int) *ClusterRing {
	cluster.nextClusterRing = NewHashRing(cluster.keyspace, cluster.dataCenter, expectedSize, replicationFactor)
	return cluster.nextClusterRing
}

func (cluster *ClusterRing) GetNextClusterRing() *ClusterRing {
	return cluster.nextClusterRing
}

func (cluster *ClusterRing) RemoveNextClusterRing() {
	cluster.nextClusterRing = nil
}

func (cluster *ClusterRing) SetReplicationFactor(replicationFactor int) {
	if replicationFactor > 0 {
		cluster.replicationFactor = replicationFactor
	}
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
	clusterSize := len(cluster.nodes)
	for _, option := range options {
		index, replica = option(index, clusterSize)
	}
	if index < 0 || index >= len(cluster.nodes) {
		return nil, 0, false
	}
	if cluster.nodes[index] == nil {
		return nil, 0, false
	}
	return cluster.nodes[index], replica, true
}

func (cluster *ClusterRing) GetOneNode(shardId int, options ...AccessOption) (Node, int, bool) {
	replica := 0
	serverId := shardId
	clusterSize := len(cluster.nodes)
	for _, option := range options {
		serverId, replica = option(shardId, clusterSize)
	}
	if serverId < 0 || serverId >= len(cluster.nodes) {
		return nil, 0, false
	}
	if cluster.nodes[serverId] == nil {
		if replica == 0 {
			// try other locations if replica is not specified
			for i := 1; i < cluster.replicationFactor; i++ {
				position := serverId + i
				if position >= cluster.expectedSize {
					position -= cluster.expectedSize
				}
				if cluster.nodes[position] != nil {
					return cluster.nodes[position], i, true
				}
			}
		}
		return nil, 0, false
	}
	return cluster.nodes[serverId], replica, true
}

// NewHashRing creates a new hash ring.
func NewHashRing(keyspace, dataCenter string, expectedSize int, replicationFactor int) *ClusterRing {
	return &ClusterRing{
		keyspace:          keyspace,
		dataCenter:        dataCenter,
		nodes:             make([]Node, expectedSize),
		expectedSize:      expectedSize,
		replicationFactor: replicationFactor,
	}
}

func (cluster *ClusterRing) String() string {
	var output bytes.Buffer
	output.Write([]byte{'['})
	for i := 0; i < len(cluster.nodes); i++ {
		if i != 0 {
			output.Write([]byte{' '})
		}
		n := cluster.nodes[i]
		if n == nil || n.GetAddress() == "" {
			output.Write([]byte{'_'})
		} else {
			output.WriteString(fmt.Sprintf("%d", n.GetId()))
		}
	}
	output.Write([]byte{']'})
	output.WriteString(fmt.Sprintf(" size %d/%d ", cluster.CurrentSize(), cluster.ExpectedSize()))

	return output.String()
}

func (cluster *ClusterRing) GetNodes() []Node {
	return cluster.nodes
}
