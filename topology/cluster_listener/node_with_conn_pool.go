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
	id           int
	network      string
	address      string
	adminAddress string
	shards       map[shard_id]*pb.ShardStatus
	p            pool.Pool
}

func newNodeWithConnPool(id int, network, address, adminAddress string) *NodeWithConnPool {
	p, _ := pool.NewChannelPool(0, 100,
		func() (net.Conn, error) {
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
		id:           id,
		network:      network,
		address:      address,
		adminAddress: adminAddress,
		shards:       make(map[shard_id]*pb.ShardStatus),
		p:            p,
	}
}

func (n *NodeWithConnPool) GetId() int {
	return n.id
}

func (n *NodeWithConnPool) GetNetwork() string {
	return n.network
}

func (n *NodeWithConnPool) GetAddress() string {
	return n.address
}

func (n *NodeWithConnPool) GetAdminAddress() string {
	return n.adminAddress
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

func (clusterListener *ClusterListener) AddNode(keyspace string, n *pb.ClusterNode) (oldShardStatus *pb.ShardStatus) {
	cluster := clusterListener.GetClusterRing(keyspace)
	st, ss := n.StoreResource, n.ShardStatus
	node, _, found := cluster.GetNode(int(ss.NodeId))
	if !found {
		node = topology.Node(newNodeWithConnPool(int(ss.NodeId), st.Network, st.Address, st.AdminAddress))
	}
	oldShardStatus = node.SetShardStatus(ss)
	cluster.Add(node)
	return oldShardStatus
}

func (clusterListener *ClusterListener) RemoveNode(keyspace string, n *pb.ClusterNode) {
	r := clusterListener.GetClusterRing(keyspace)
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
