package topology

import (
	"github.com/dgryski/go-jump"
)

type Node struct {
	id   int
	host string
}

func (n *Node) GetId() int {
	return n.id
}

func (n *Node) GetHost() string {
	return n.host
}

func NewNode(id int, host string) *Node {
	return &Node{id: id, host: host}
}

// --------------------
//      Hash Cluster
// --------------------

type ClusterRing struct {
	dataCenter         string
	nodes              []*Node
	currentClusterSize int
	nextClusterSize    int
}

// adds a host (+virtual hosts to the ring)
func (h *ClusterRing) Add(n *Node) {
	if len(h.nodes) < n.GetId()+1 {
		cap := n.GetId() + 1
		nodes := make([]*Node, cap)
		copy(nodes, h.nodes)
		h.nodes = nodes
	}
	h.nodes[n.GetId()] = n
}

func (h *ClusterRing) Remove(n *Node) {
	if n.GetId() < len(h.nodes) {
		h.nodes[n.GetId()] = nil
	}
}

// calculates a Jump hash for the key provided
func (h *ClusterRing) FindBucketGivenSize(key uint64, size int) int {
	return int(jump.Hash(key, size))
}

// calculates a Jump hash for the key provided
func (h *ClusterRing) FindBucket(key uint64) int {
	return int(jump.Hash(key, h.CurrentSize()))
}

// returns the size of the ring
func (h *ClusterRing) CurrentSize() int {
	return h.currentClusterSize
}

func (h *ClusterRing) NextSize() int {
	return h.nextClusterSize
}

func (h *ClusterRing) NodeCount() int {
	return len(h.nodes)
}

// returns a particular index
func (h *ClusterRing) GetNode(index int) *Node {
	return h.nodes[index]
}

func (h *ClusterRing) GetDataCenter() string {
	return h.dataCenter
}

// NewHashRing creates a new hash ring.
func NewHashRing(dataCenter string) *ClusterRing {
	return &ClusterRing{
		dataCenter: dataCenter,
		nodes:      make([]*Node, 0, 16),
	}
}
