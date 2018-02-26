package cluster_listener

import (
	"fmt"
	"github.com/chrislusf/glog"
	"github.com/chrislusf/vasto/util"
	"gopkg.in/fatih/pool.v2"
	"net"
	"time"
)

/*
 * Functions here are used by clients to read/write with the data store.
 * If a shard has a candidate, use the candidate.
 */

func (clusterListener *ClusterListener) GetConnectionByShardId(keyspace string, shardId int, replica int) (net.Conn, error) {

	r, found := clusterListener.GetCluster(keyspace)
	if !found {
		return nil, fmt.Errorf("no keyspace %s", keyspace)
	}

	// find one shard
	n, ok := r.GetNode(shardId, replica)
	if !ok {
		return nil, fmt.Errorf("shardId %d not found", shardId)
	}

	clusterListener.connPoolLock.Lock()
	connPool, foundPool := clusterListener.connPools[n.StoreResource.Address]
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
		clusterListener.connPools[n.StoreResource.Address] = connPool
	}
	clusterListener.connPoolLock.Unlock()

	conn, err := connPool.Get()
	if err != nil {
		return nil, fmt.Errorf("GetConnection shard %d %s %+v", shardId, n.StoreResource.Address, err)
	}

	if clusterListener.verbose {
		if replica > 0 {
			glog.V(2).Infof("connecting to server %d at %s replica=%d", shardId, n.StoreResource.Address, replica)
		}
	}

	return conn, nil

}

func (clusterListener *ClusterListener) GetShardId(keyspace string, partitionKey []byte) (shardId int, partitionHash uint64) {
	partitionHash = util.Hash(partitionKey)
	r, found := clusterListener.GetCluster(keyspace)
	if !found {
		return -1, partitionHash
	}
	shardId = r.FindShardId(partitionHash)
	return
}
