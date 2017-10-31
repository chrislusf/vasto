package cluster_listener

import (
	"fmt"
	"github.com/chrislusf/vasto/util"
	"net"
)

func (c *ClusterListener) GetConnectionByPartitionKey(partitionKey []byte) (net.Conn, error) {
	partitionHash := util.Hash(partitionKey)
	return c.GetConnectionByPartitionHash(partitionHash)
}

func (c *ClusterListener) GetConnectionByPartitionHash(partitionHash uint64) (net.Conn, error) {
	bucket := c.FindBucket(partitionHash)
	return c.GetConnectionByBucket(bucket)
}

func (c *ClusterListener) GetConnectionByBucket(bucket int) (net.Conn, error) {
	n := c.GetNode(bucket)

	node, ok := n.(*NodeWithConnPool)

	if !ok {
		return nil, fmt.Errorf("unexpected node %+v", n)
	}

	conn, err := node.GetConnection()
	if err != nil {
		return nil, fmt.Errorf("GetConnection node %d %s %+v", n.GetId(), n.GetAddress(), err)
	}
	return conn, nil
}
