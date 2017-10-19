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
}

type node struct {
	id      int
	network string
	address string
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

func NewNode(id int, network, address string) Node {
	return &node{id: id, network: network, address: address}
}

// --------------------
//      Hash FixedCluster
// --------------------

type ClusterRing struct {
	dataCenter         string
	nodes              []Node
	currentClusterSize int
	nextClusterSize    int
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
	return int(jump.Hash(key, h.CurrentSize()))
}

func (h *ClusterRing) CurrentSize() int {
	return h.currentClusterSize
}

func (h *ClusterRing) NextSize() int {
	return h.nextClusterSize
}

func (h *ClusterRing) SetCurrentSize(currentSize int) {
	h.currentClusterSize = currentSize
}

func (h *ClusterRing) SetNextSize(nextSize int) {
	h.nextClusterSize = nextSize
}

func (h *ClusterRing) NodeCount() int {
	for i := len(h.nodes); i > 0; i-- {
		if h.nodes[i-1] == nil || h.nodes[i-1].GetAddress() == "" {
			continue
		}
		return i
	}
	return 0
}

// returns a particular index
func (h *ClusterRing) GetNode(index int) Node {
	return h.nodes[index]
}

func (h *ClusterRing) GetDataCenter() string {
	return h.dataCenter
}

func (h *ClusterRing) MissingAndFreeNodeIds() (missingList, freeList []int) {
	max := len(h.nodes)
	if max < h.currentClusterSize {
		max = h.currentClusterSize
	}
	for i := 0; i < max; i++ {
		var n Node
		if i < len(h.nodes) {
			n = h.nodes[i]
		}
		if n == nil || n.GetAddress() == "" {
			if i < h.currentClusterSize {
				missingList = append(missingList, i)
			}
		} else {
			if i >= h.currentClusterSize {
				freeList = append(freeList, i)
			}
		}
	}
	return
}

// NewHashRing creates a new hash ring.
func NewHashRing(dataCenter string) *ClusterRing {
	return &ClusterRing{
		dataCenter: dataCenter,
		nodes:      make([]Node, 0, 16),
	}
}

func (h *ClusterRing) String() string {
	var output bytes.Buffer
	output.Write([]byte{'['})
	nodeCount := h.NodeCount()
	max := len(h.nodes)
	if max < h.currentClusterSize {
		max = h.currentClusterSize
	}
	for i := 0; i < max; i++ {
		var n Node
		if i < len(h.nodes) {
			n = h.nodes[i]
		}
		if n == nil || n.GetAddress() == "" {
			if i < h.currentClusterSize || i < nodeCount {
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
		output.WriteString(fmt.Sprintf(" size %d ", h.currentClusterSize))
	} else {
		output.WriteString(fmt.Sprintf(" size %d=>%d ", h.currentClusterSize, h.nextClusterSize))
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
