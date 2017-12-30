package cluster_listener

import (
	"fmt"
	"github.com/chrislusf/vasto/topology"
	"github.com/chrislusf/vasto/util"
	"net"
	"log"
)

/*
 * Functions here are used by clients to read/write with the data store.
 * If a shard has a candidate, use the candidate.
 */

func (clusterListener *ClusterListener) GetConnectionByShardId(keyspace string, shardId int, options ...topology.AccessOption) (net.Conn, int, error) {

	r, found := clusterListener.GetClusterRing(keyspace)
	if !found {
		return nil, 0, fmt.Errorf("no keyspace %s", keyspace)
	}

	// find one shard
	n, replica, ok := r.GetOneNode(shardId, options...)
	if !ok {
		return nil, 0, fmt.Errorf("shardId %d not found", shardId)
	}

	if r.GetNextClusterRing() != nil {
		candidate, _, found := r.GetNextClusterRing().GetNode(shardId, options...)
		if found {
			if clusterListener.verbose {
				log.Printf("connecting to candidate %s", candidate.GetAddress())
			}
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

	if clusterListener.verbose {
		if replica > 0 {
			log.Printf("connecting to server %d at %s replica=%d", node.id, node.GetAddress(), replica)
		}
	}

	return conn, replica, nil

}

func (clusterListener *ClusterListener) GetShardId(keyspace string, partitionKey []byte) (shardId int, partitionHash uint64) {
	partitionHash = util.Hash(partitionKey)
	r, found := clusterListener.GetClusterRing(keyspace)
	if !found {
		return -1, partitionHash
	}
	shardId = r.FindShardId(partitionHash)
	return
}
