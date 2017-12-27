package cluster_listener

import (
	"fmt"
	"github.com/chrislusf/vasto/topology"
	"github.com/chrislusf/vasto/util"
	"net"
)

/*
 * Functions here are used by clients to read/write with the data store.
 * If a shard has a candidate, use the candidate.
 */

func (clusterListener *ClusterListener) GetConnectionByPartitionKey(keyspace string, partitionKey []byte, options ...topology.AccessOption) (net.Conn, int, error) {
	partitionHash := util.Hash(partitionKey)
	return clusterListener.GetConnectionByPartitionHash(keyspace, partitionHash, options...)
}

func (clusterListener *ClusterListener) GetConnectionByPartitionHash(keyspace string, partitionHash uint64, options ...topology.AccessOption) (net.Conn, int, error) {
	r, found := clusterListener.GetClusterRing(keyspace)
	if !found {
		return nil, 0, fmt.Errorf("no keyspace %s", keyspace)
	}
	bucket := r.FindBucket(partitionHash)
	return clusterListener.GetConnectionByBucket(keyspace, bucket, options...)
}

func (clusterListener *ClusterListener) GetConnectionByBucket(keyspace string, bucket int, options ...topology.AccessOption) (net.Conn, int, error) {

	r, found := clusterListener.GetClusterRing(keyspace)
	if !found {
		return nil, 0, fmt.Errorf("no keyspace %s", keyspace)
	}

	n, replica, ok := r.GetOneNode(bucket, options...)
	if !ok {
		return nil, 0, fmt.Errorf("bucket %d not found", bucket)
	}

	if r.GetNextClusterRing() != nil {
		candidate, _, found := r.GetNextClusterRing().GetNode(bucket, options...)
		if found {
			// TODO remove this
			println("connecting to candidate", candidate.GetAddress())
			n = candidate
		}
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
		// TODO remove this
		println("connecting to", node.id, node.GetAddress(), "replica =", replica)
	}

	return conn, replica, nil

}
