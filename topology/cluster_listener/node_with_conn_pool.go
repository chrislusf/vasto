package cluster_listener

import (
	"net"
	"time"

	"fmt"
	"github.com/chrislusf/vasto/pb"
	"gopkg.in/fatih/pool.v2"
	"github.com/chrislusf/vasto/topology"
	"github.com/chrislusf/vasto/util"
)

type shard_id uint32

type NodeWithConnPool struct {
	id              int
	network         string
	address         string
	adminAddress    string
	storeResource   *pb.StoreResource
	shards          map[shard_id]*pb.ShardStatus
	p               pool.Pool
	alternativeNode topology.Node
}

func newNodeWithConnPool(id int, storeResource *pb.StoreResource) *NodeWithConnPool {
	p, _ := pool.NewChannelPool(0, 100,
		func() (net.Conn, error) {
			network, address := storeResource.Network, storeResource.Address
			if unixSocket, ok := util.GetUnixSocketFile(address); ok {
				network, address = "unix", unixSocket
			}

			conn, err := net.Dial(network, address)
			if err != nil {
				return nil, fmt.Errorf("Failed to dial %s on %s : %v", network, address, err)
			}
			conn.SetDeadline(time.Time{})
			if c, ok := conn.(*net.TCPConn); ok {
				c.SetKeepAlive(true)
				c.SetNoDelay(true)
			}
			return conn, err
		})
	return &NodeWithConnPool{
		id:            id,
		storeResource: storeResource,
		shards:        make(map[shard_id]*pb.ShardStatus),
		p:             p,
	}
}

func (n *NodeWithConnPool) GetId() int {
	return n.id
}

func (n *NodeWithConnPool) GetNetwork() string {
	return n.storeResource.Network
}

func (n *NodeWithConnPool) GetAddress() string {
	return n.storeResource.Address
}

func (n *NodeWithConnPool) GetAdminAddress() string {
	return n.storeResource.AdminAddress
}

func (n *NodeWithConnPool) GetStoreResource() *pb.StoreResource {
	return n.storeResource
}

func (n *NodeWithConnPool) GetConnection() (net.Conn, error) {
	return n.p.Get()
}

func (n *NodeWithConnPool) SetShardStatus(shardStatus *pb.ShardStatus) (oldShardStatus *pb.ShardStatus) {
	oldShardStatus = n.shards[shard_id(shardStatus.ShardId)]
	n.shards[shard_id(shardStatus.ShardId)] = shardStatus
	return
}

func (n *NodeWithConnPool) RemoveShardStatus(shardStatus *pb.ShardStatus) {
	delete(n.shards, shard_id(shardStatus.ShardId))
}

func (n *NodeWithConnPool) GetShardStatuses() []*pb.ShardStatus {
	var statuses []*pb.ShardStatus
	for _, shard := range n.shards {
		ss := shard
		statuses = append(statuses, ss)
	}
	return statuses
}

func (n *NodeWithConnPool) GetAlternativeNode() topology.Node {
	return n.alternativeNode
}

func (n *NodeWithConnPool) SetAlternativeNode(alt topology.Node) {
	n.alternativeNode = alt
}

func (clusterListener *ClusterListener) AddNode(keyspace string, n *pb.ClusterNode) (oldShardStatus *pb.ShardStatus) {
	cluster := clusterListener.GetOrSetClusterRing(keyspace, int(n.ShardStatus.ClusterSize), int(n.ShardStatus.ReplicationFactor))
	st, ss := n.StoreResource, n.ShardStatus
	node, _, found := cluster.GetNode(int(ss.NodeId))
	if !found {
		node = topology.Node(newNodeWithConnPool(int(ss.NodeId), st))
	}
	oldShardStatus = node.SetShardStatus(ss)
	cluster.Add(node)
	return oldShardStatus
}

func (clusterListener *ClusterListener) RemoveNode(keyspace string, n *pb.ClusterNode) {
	r, found := clusterListener.GetClusterRing(keyspace)
	if !found {
		return
	}
	ss := n.ShardStatus
	if n != nil {
		node := r.Remove(int(ss.NodeId))
		if node != nil {
			node.RemoveShardStatus(ss)
			if t, ok := node.(*NodeWithConnPool); ok {
				if len(t.shards) == 0 {
					t.p.Close()
				}
			}
		}
	}
}
