package cluster_listener

import (
	"fmt"
	"github.com/chrislusf/vasto/topology"
	"github.com/chrislusf/vasto/util"
	"net"
	"log"
	"time"
	"gopkg.in/fatih/pool.v2"
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
	n, replica, ok := r.GetNode(shardId, options...)
	if !ok {
		return nil, 0, fmt.Errorf("shardId %d not found", shardId)
	}

	if r.GetNextCluster() != nil {
		candidate, _, found := r.GetNextCluster().GetNode(shardId, options...)
		if found {
			if clusterListener.verbose {
				log.Printf("connecting to candidate %s", candidate.StoreResource.Address)
			}
			n = candidate
		}
	}

	clusterListener.connPoolLock.RLock()
	connPool, foundPool := clusterListener.connPools[n.StoreResource.Address]
	clusterListener.connPoolLock.RUnlock()
	if !foundPool {
		connPool, _ = pool.NewChannelPool(0, 100,
			func() (net.Conn, error) {
				network, address := n.StoreResource.Network, n.StoreResource.Address
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
		clusterListener.connPoolLock.Lock()
		clusterListener.connPools[n.StoreResource.Address] = connPool
		clusterListener.connPoolLock.Unlock()
	}

	conn, err := connPool.Get()
	if err != nil {
		return nil, 0, fmt.Errorf("GetConnection shard %d %s %+v", shardId, n.StoreResource.Address, err)
	}

	if clusterListener.verbose {
		if replica > 0 {
			log.Printf("connecting to server %d at %s replica=%d", shardId, n.StoreResource.Address, replica)
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
