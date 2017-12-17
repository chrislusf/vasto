package topology

import (
	"github.com/chrislusf/vasto/pb"
)

type Node interface {
	GetId() int
	GetNetwork() string
	GetAddress() string
	GetAdminAddress() string
	GetStoreResource() *pb.StoreResource
	SetShardStatus(shardStatus *pb.ShardStatus) (oldShardStatus *pb.ShardStatus)
	RemoveShardStatus(shardStatus *pb.ShardStatus)
	GetShardStatuses() []*pb.ShardStatus
	GetAlternativeNode() Node
	SetAlternativeNode(Node)
}

type node struct {
	id              int
	store           *pb.StoreResource
	shards          map[string]*pb.ShardStatus
	alternativeNode Node
}

func (n *node) GetId() int {
	return n.id
}

func (n *node) GetNetwork() string {
	return n.store.Network
}

func (n *node) GetAddress() string {
	return n.store.Address
}

func (n *node) GetAdminAddress() string {
	return n.store.AdminAddress
}

func (n *node) GetStoreResource() *pb.StoreResource {
	return n.store
}

func NewNode(id int, store *pb.StoreResource) Node {
	return &node{id: id, store: store, shards: make(map[string]*pb.ShardStatus)}
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

func (n *node) GetAlternativeNode() Node {
	return n.alternativeNode
}

func (n *node) SetAlternativeNode(alt Node) {
	n.alternativeNode = alt
}
