package cluster_listener

import (
	"fmt"
	"github.com/chrislusf/vasto/topology"
	"github.com/chrislusf/vasto/util"
	"net"
)

func (c *ClusterListener) GetConnectionByPartitionKey(keyspace string, partitionKey []byte, options ...topology.AccessOption) (net.Conn, int, error) {
	partitionHash := util.Hash(partitionKey)
	return c.GetConnectionByPartitionHash(keyspace, partitionHash, options...)
}

func (c *ClusterListener) GetConnectionByPartitionHash(keyspace string, partitionHash uint64, options ...topology.AccessOption) (net.Conn, int, error) {
	r := c.GetClusterRing(keyspace)
	bucket := r.FindBucket(partitionHash)
	return c.GetConnectionByBucket(keyspace, bucket, options...)
}

func (c *ClusterListener) GetConnectionByBucket(keyspace string, bucket int, options ...topology.AccessOption) (net.Conn, int, error) {

	r := c.GetClusterRing(keyspace)

	n, replica, ok := r.GetNode(bucket, options...)
	if !ok {
		return nil, 0, fmt.Errorf("bucket %d not found", bucket)
	}

	node, ok := n.(*NodeWithConnPool)
	if !ok {
		return nil, 0, fmt.Errorf("unexpected node %+v", n)
	}

	conn, err := node.GetConnection()
	if err != nil {
		return nil, 0, fmt.Errorf("GetConnection node %d %s %+v", n.GetId(), n.GetAddress(), err)
	}

	if replica > 0 {
		println("connecting to", node.id, node.GetAddress(), "replica =", replica)
	}

	return conn, replica, nil

}
