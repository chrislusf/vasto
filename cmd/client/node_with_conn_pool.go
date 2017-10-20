package client

import (
	"log"
	"net"
	"time"

	"fmt"
	"github.com/chrislusf/vasto/pb"
	"gopkg.in/fatih/pool.v2"
)

type nodeWithConnPool struct {
	id      int
	network string
	address string
	p       pool.Pool
}

func newNodeWithConnPool(id int, network, address string) *nodeWithConnPool {
	p, _ := pool.NewChannelPool(0, 100,
		func() (net.Conn, error) {
			conn, err := net.Dial(network, address)
			println("connecting to", network, address)
			if err != nil {
				fmt.Printf("Failed to dial %s on %s : %v", network, address, err)
			}
			conn.SetDeadline(time.Time{})
			if c, ok := conn.(*net.TCPConn); ok {
				c.SetKeepAlive(true)
				c.SetNoDelay(true)
			}
			return conn, err
		})
	return &nodeWithConnPool{
		id:      id,
		network: network,
		address: address,
		p:       p,
	}
}

func (n *nodeWithConnPool) GetId() int {
	return n.id
}

func (n *nodeWithConnPool) GetNetwork() string {
	return n.network
}

func (n *nodeWithConnPool) GetAddress() string {
	return n.address
}

func (n *nodeWithConnPool) GetConnection() (net.Conn, error) {
	return n.p.Get()
}

func (c *VastoClient) AddNode(store *pb.StoreResource) {
	node := newNodeWithConnPool(int(store.Id), store.Network, store.Address)
	c.cluster.Add(node)
	log.Printf("+node %d: %s:%s, cluster: %s", node.GetId(), node.GetNetwork(), node.GetAddress(), c.cluster)
}

func (c *VastoClient) RemoveNode(store *pb.StoreResource) {
	n := c.cluster.Remove(int(store.Id))
	if n != nil {
		if t, ok := n.(*nodeWithConnPool); ok {
			t.p.Close()
		}
	}
	log.Printf("-node %d: %s, cluster: %s", store.GetId(), store.Address, c.cluster)
}
