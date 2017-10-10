package topology

import (
	jump "github.com/dgryski/go-jump"
)

type Node interface {
	GetId() int
	GetHost() string
}

type Ring interface {
	Add(n Node)
	Remove(n Node)
	GetDataCenter() string

	FindBucket(key uint64) int

	// Finds a bucket for a given key based on the size of the ring given.
	FindBucketGivenSize(key uint64, size int) int

	// Returns the size of the ring. Virtual nodes are included.
	Size() int

	// Returns a node for the given bucket number
	GetNode(index int) Node
}

// --------------------
//      Objects
// --------------------

// Node implementation
type node struct {
	id   int
	host string
}

func (n node) GetId() int {
	return n.id
}

func (n node) GetHost() string {
	return n.host
}

func NewNode(id int, host string) Node {
	return node{id: id, host: host}
}

// --------------------
//      Hash Ring
// --------------------

type hashRing struct {
	dataCenter string
	nodes      []Node
}

// adds a host (+virtual hosts to the ring)
func (h *hashRing) Add(n Node) {
	if len(h.nodes) < n.GetId()+1 {
		cap := n.GetId() + 1
		nodes := make([]Node, cap)
		copy(nodes, h.nodes)
		h.nodes = nodes
	}
	h.nodes[n.GetId()] = n
}

func (h *hashRing) Remove(n Node) {
	if n.GetId() < len(h.nodes) {
		h.nodes[n.GetId()] = nil
	}
}

// calculates a Jump hash for the key provided
func (h *hashRing) FindBucketGivenSize(key uint64, size int) int {
	return int(jump.Hash(key, size))
}

// calculates a Jump hash for the key provided
func (h *hashRing) FindBucket(key uint64) int {
	return int(jump.Hash(key, h.Size()))
}

// returns the size of the ring
func (h *hashRing) Size() int {
	return len(h.nodes)
}

// returns a particular index
func (h *hashRing) GetNode(index int) Node {
	return h.nodes[index]
}

func (h *hashRing) GetDataCenter() string {
	return h.dataCenter
}

// NewHashRing creates a new hash ring.
func NewHashRing(dataCenter string) Ring {
	return &hashRing{
		dataCenter: dataCenter,
		nodes:      make([]Node, 0, 16),
	}
}
