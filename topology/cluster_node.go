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
	SetShardInfo(ShardInfo *pb.ShardInfo) (oldShardInfo *pb.ShardInfo)
	RemoveShardInfo(ShardInfo *pb.ShardInfo)
	GetShardInfoes() []*pb.ShardInfo
	GetAlternativeNode() Node
	SetAlternativeNode(Node)
}

type node struct {
	id              int
	store           *pb.StoreResource
	shards          map[string]*pb.ShardInfo
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
	return &node{id: id, store: store, shards: make(map[string]*pb.ShardInfo)}
}

func (n *node) SetShardInfo(ShardInfo *pb.ShardInfo) (oldShardInfo *pb.ShardInfo) {
	oldShardInfo = n.shards[ShardInfo.IdentifierOnThisServer()]
	n.shards[ShardInfo.IdentifierOnThisServer()] = ShardInfo
	return
}

func (n *node) RemoveShardInfo(ShardInfo *pb.ShardInfo) {
	delete(n.shards, ShardInfo.IdentifierOnThisServer())
}

func (n *node) GetShardInfoes() []*pb.ShardInfo {
	var statuses []*pb.ShardInfo
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
