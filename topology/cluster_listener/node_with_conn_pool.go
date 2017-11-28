package cluster_listener

import (
	"log"
	"net"
	"time"

	"fmt"
	"github.com/chrislusf/vasto/pb"
	"gopkg.in/fatih/pool.v2"
)

type NodeWithConnPool struct {
	id           int
	network      string
	address      string
	adminAddress string
	p            pool.Pool
}

func newNodeWithConnPool(id int, network, address, adminAddress string) *NodeWithConnPool {
	p, _ := pool.NewChannelPool(0, 100,
		func() (net.Conn, error) {
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

func (c *ClusterListener) AddNode(n *pb.ClusterNode) {
	node := newNodeWithConnPool(int(n.ShardId), n.Network, n.Address, n.AdminAddress)
	c.Add(node)
	log.Printf("+node %d: %s:%s, cluster: %s", node.GetId(), node.GetNetwork(), node.GetAddress(), c)
}

func (c *ClusterListener) RemoveNode(n *pb.ClusterNode) {
	node := c.Remove(int(n.ShardId))
	if n != nil {
		if t, ok := node.(*NodeWithConnPool); ok {
			t.p.Close()
		}
	}
	log.Printf("-node %d: %s, cluster: %s", node.GetId(), node.GetAddress(), c)
}
