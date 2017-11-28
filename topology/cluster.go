package topology

import (
	"bytes"
	"fmt"

	"github.com/dgryski/go-jump"
)

type Node interface {
	GetId() int
	GetNetwork() string
	GetAddress() string
	GetAdminAddress() string
}

type node struct {
	id           int
	network      string
	address      string
	adminAddress string
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
	return &node{id: id, network: network, address: address, adminAddress: adminAddress}
}

// --------------------
//      Hash FixedCluster
// --------------------

type ClusterRing struct {
	keyspace            string
	dataCenter          string
	nodes               []Node
	expectedClusterSize int
	nextClusterSize     int
}

// adds a address (+virtual hosts to the ring)
func (h *ClusterRing) Add(n Node) {
	if len(h.nodes) < n.GetId()+1 {
		cap := n.GetId() + 1
		nodes := make([]Node, cap)
		copy(nodes, h.nodes)
		h.nodes = nodes
	}
	h.nodes[n.GetId()] = n
}

func (h *ClusterRing) Remove(shardId int) Node {
	if shardId < len(h.nodes) {
		n := h.nodes[shardId]
		h.nodes[shardId] = nil
		return n
	}
	return nil
}

// calculates a Jump hash for the key provided
func (h *ClusterRing) FindBucketGivenSize(key uint64, size int) int {
	return int(jump.Hash(key, size))
}

// calculates a Jump hash for the key provided
func (h *ClusterRing) FindBucket(key uint64) int {
	return int(jump.Hash(key, h.ExpectedSize()))
}

func (h *ClusterRing) ExpectedSize() int {
	return h.expectedClusterSize
}

func (h *ClusterRing) NextSize() int {
	return h.nextClusterSize
}

func (h *ClusterRing) SetExpectedSize(expectedSize int) {
	h.expectedClusterSize = expectedSize
}

func (h *ClusterRing) SetNextSize(nextSize int) {
	h.nextClusterSize = nextSize
}

func (h *ClusterRing) CurrentSize() int {
	for i := len(h.nodes); i > 0; i-- {
		if h.nodes[i-1] == nil || h.nodes[i-1].GetAddress() == "" {
			continue
		}
		return i
	}
	return 0
}

func (h *ClusterRing) GetNode(index int, options ...AccessOption) (Node, int, bool) {
	replica := 0
	for _, option := range options {
		index, replica = option(index)
	}
	if index < 0 || index >= len(h.nodes) {
		return nil, 0, false
	}
	return h.nodes[index], replica, true
}

func (h *ClusterRing) MissingAndFreeNodeIds() (missingList, freeList []int) {
	max := len(h.nodes)
	currentClusterSize := h.CurrentSize()
	if max < currentClusterSize {
		max = currentClusterSize
	}
	for i := 0; i < max; i++ {
		var n Node
		if i < len(h.nodes) {
			n = h.nodes[i]
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
func NewHashRing(keyspace, dataCenter string) *ClusterRing {
	return &ClusterRing{
		keyspace:   keyspace,
		dataCenter: dataCenter,
		nodes:      make([]Node, 0, 16),
	}
}

func (h *ClusterRing) String() string {
	var output bytes.Buffer
	output.Write([]byte{'['})
	nodeCount := h.CurrentSize()
	max := len(h.nodes)
	if max < h.expectedClusterSize {
		max = h.expectedClusterSize
	}
	for i := 0; i < max; i++ {
		var n Node
		if i < len(h.nodes) {
			n = h.nodes[i]
		}
		if n == nil || n.GetAddress() == "" {
			if i < h.expectedClusterSize || i < nodeCount {
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
	if h.nextClusterSize == 0 {
		output.WriteString(fmt.Sprintf(" size %d->%d ", h.CurrentSize(), h.expectedClusterSize))
	} else {
		output.WriteString(fmt.Sprintf(" size %d=>%d ", h.CurrentSize(), h.nextClusterSize))
	}

	missingList, freeList := h.MissingAndFreeNodeIds()

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
