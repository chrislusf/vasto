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
	id            int
	network       string
	address       string
	adminAddress  string
	storeResource *pb.StoreResource
	shards        map[shard_id]*pb.ShardInfo
	p             pool.Pool
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
		shards:        make(map[shard_id]*pb.ShardInfo),
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

func (n *NodeWithConnPool) SetShardInfo(ShardInfo *pb.ShardInfo) (oldShardInfo *pb.ShardInfo) {
	oldShardInfo = n.shards[shard_id(ShardInfo.ShardId)]
	n.shards[shard_id(ShardInfo.ShardId)] = ShardInfo
	return
}

func (n *NodeWithConnPool) RemoveShardInfo(ShardInfo *pb.ShardInfo) {
	delete(n.shards, shard_id(ShardInfo.ShardId))
}

func (n *NodeWithConnPool) GetShardInfoList() []*pb.ShardInfo {
	var statuses []*pb.ShardInfo
	for _, shard := range n.shards {
		ss := shard
		statuses = append(statuses, ss)
	}
	return statuses
}

func (clusterListener *ClusterListener) AddNode(keyspace string, n *pb.ClusterNode) (oldShardInfo *pb.ShardInfo) {
	cluster := clusterListener.GetOrSetClusterRing(keyspace, int(n.ShardInfo.ClusterSize), int(n.ShardInfo.ReplicationFactor))

	if n.ShardInfo.IsCandidate {
		if cluster.GetNextClusterRing() == nil {
			cluster.SetNextClusterRing(int(n.ShardInfo.ClusterSize), int(n.ShardInfo.ReplicationFactor))
		}
		cluster = cluster.GetNextClusterRing()
	}

	st, ss := n.StoreResource, n.ShardInfo
	node, _, found := cluster.GetNode(int(ss.ServerId))
	if !found {
		node = topology.Node(newNodeWithConnPool(int(ss.ServerId), st))
	}
	oldShardInfo = node.SetShardInfo(ss)
	cluster.SetNode(node)
	return oldShardInfo
}

func (clusterListener *ClusterListener) RemoveNode(keyspace string, n *pb.ClusterNode) {
	cluster, found := clusterListener.GetClusterRing(keyspace)
	if !found {
		return
	}
	if n.ShardInfo.IsCandidate {
		if cluster.GetNextClusterRing() == nil {
			return
		}
		cluster = cluster.GetNextClusterRing()
	}
	ss := n.ShardInfo
	if n != nil {
		node := cluster.RemoveNode(int(ss.ServerId))
		if node != nil {
			node.RemoveShardInfo(ss)
			if t, ok := node.(*NodeWithConnPool); ok {
				if len(t.shards) == 0 {
					t.p.Close()
				}
			}
		}
	}
}

func (clusterListener *ClusterListener) PromoteNode(keyspace string, n *pb.ClusterNode) {
	cluster, foundCluster := clusterListener.GetClusterRing(keyspace)
	if !foundCluster {
		return
	}
	if cluster.GetNextClusterRing() == nil {
		return
	}

	candidateCluster := cluster.GetNextClusterRing()

	if n != nil {
		candidateCluster.RemoveShardInfo(n.ShardInfo)
		if candidateCluster.CurrentSize() == 0 {
			cluster.RemoveNextClusterRing()
		}
		node, _, found := cluster.GetNode(int(n.ShardInfo.ServerId))
		if !found {
			node = newNodeWithConnPool(int(n.ShardInfo.ServerId), n.StoreResource)
			cluster.SetNode(node)
		}
		node.SetShardInfo(n.ShardInfo)
	}
}
