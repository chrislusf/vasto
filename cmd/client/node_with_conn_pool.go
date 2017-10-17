package client

import (
	"net"

	"fmt"
	"github.com/chrislusf/vasto/pb"
	"gopkg.in/fatih/pool.v2"
)

type nodeWithConnPool struct {
	id   int
	host string
	p    pool.Pool
}

func newNodeWithConnPool(store *pb.StoreResource) *nodeWithConnPool {
	p, _ := pool.NewChannelPool(0, 2,
		func() (net.Conn, error) {
			return net.Dial("tcp", store.Address)
		})
	return &nodeWithConnPool{
		id:   int(store.Id),
		host: store.Address,
		p:    p,
	}
}

func (n *nodeWithConnPool) GetId() int {
	return n.id
}

func (n *nodeWithConnPool) GetHost() string {
	return n.host
}

func (n *nodeWithConnPool) GetConnection() (net.Conn, error) {
	return n.p.Get()
}

func (c *VastoClient) AddNode(store *pb.StoreResource) {
	node := newNodeWithConnPool(store)
	c.cluster.Add(node)
	fmt.Printf("   add node %d: %v\n", node.GetId(), node.GetHost())
}

func (c *VastoClient) RemoveNode(store *pb.StoreResource) {
	fmt.Printf("remove node %d: %v\n", store.GetId(), store.Address)
	n := c.cluster.Remove(int(store.Id))
	if n != nil {
		if t, ok := n.(*nodeWithConnPool); ok {
			t.p.Close()
		}
	}
}
