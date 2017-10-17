package topology

import (
	"github.com/dgryski/go-jump"
)

type Node interface {
	GetId() int
	GetHost() string
}

type node struct {
	id   int
	host string
}

func (n *node) GetId() int {
	return n.id
}

func (n *node) GetHost() string {
	return n.host
}

func NewNode(id int, host string) Node {
	return &node{id: id, host: host}
}

// --------------------
//      Hash Cluster
// --------------------

type ClusterRing struct {
	dataCenter         string
	nodes              []Node
	currentClusterSize int
	nextClusterSize    int
}

// adds a host (+virtual hosts to the ring)
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
	return len(h.nodes)
}

// returns a particular index
func (h *ClusterRing) GetNode(index int) Node {
	return h.nodes[index]
}

func (h *ClusterRing) GetDataCenter() string {
	return h.dataCenter
}

// NewHashRing creates a new hash ring.
func NewHashRing(dataCenter string) *ClusterRing {
	return &ClusterRing{
		dataCenter: dataCenter,
		nodes:      make([]Node, 0, 16),
	}
}
