package clusterlistener

import (
	"time"

	"context"
	"github.com/chrislusf/glog"
	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/topology"
	"github.com/chrislusf/vasto/util"
	"gopkg.in/fatih/pool.v2"
	"sync"
)

type keyspaceName string

type keyspaceFollowMessage struct {
	keyspace   keyspaceName
	isUnfollow bool
}

// ClusterListener listens to cluster topology changes, and maintains a connection pool to the servers.
type ClusterListener struct {
	sync.RWMutex
	clusters                  map[keyspaceName]*topology.Cluster
	keyspaceFollowMessageChan chan keyspaceFollowMessage
	dataCenter                string
	shardEventProcessors      []ShardEventProcessor
	clientName                string
	connPools                 map[string]pool.Pool
	connPoolLock              sync.Mutex
	disableUnixSocket         bool
}

// NewClusterListener creates a cluster listener in a data center.
// clientName is only for display purpose.
func NewClusterListener(dataCenter string, clientName string) *ClusterListener {
	return &ClusterListener{
		clusters:                  make(map[keyspaceName]*topology.Cluster),
		keyspaceFollowMessageChan: make(chan keyspaceFollowMessage, 1),
		dataCenter:                dataCenter,
		clientName:                clientName,
		connPools:                 make(map[string]pool.Pool),
	}
}

// AddExistingKeyspace registers one keyspace to listen for changes.
// This is used by store server. The keyspace should already exists in local store.
func (clusterListener *ClusterListener) AddExistingKeyspace(keyspace string, clusterSize int, replicationFactor int) {
	clusterListener.Lock()
	clusterListener.clusters[keyspaceName(keyspace)] = topology.NewCluster(keyspace, clusterListener.dataCenter, clusterSize, replicationFactor)
	clusterListener.Unlock()
}

// AddNewKeyspace registers one keyspace to listen for changes. Mostly used by client APIs.
// The keyspace should be new in local store.
func (clusterListener *ClusterListener) AddNewKeyspace(keyspace string, clusterSize int, replicationFactor int) *topology.Cluster {
	t := clusterListener.GetOrSetCluster(keyspace, clusterSize, replicationFactor)
	clusterListener.keyspaceFollowMessageChan <- keyspaceFollowMessage{keyspace: keyspaceName(keyspace)}
	return t
}

// RemoveKeyspace stops following changes of a keyspace.
func (clusterListener *ClusterListener) RemoveKeyspace(keyspace string) {
	clusterListener.Lock()
	if _, found := clusterListener.clusters[keyspaceName(keyspace)]; found {
		delete(clusterListener.clusters, keyspaceName(keyspace))
		clusterListener.keyspaceFollowMessageChan <- keyspaceFollowMessage{keyspace: keyspaceName(keyspace), isUnfollow: true}
	}
	clusterListener.Unlock()
}

// GetCluster gets the cluster of the keyspace in local data center
func (clusterListener *ClusterListener) GetCluster(keyspace string) (r *topology.Cluster, found bool) {
	clusterListener.RLock()
	r, found = clusterListener.clusters[keyspaceName(keyspace)]
	clusterListener.RUnlock()
	return
}

// GetOrSetCluster gets or creates the cluster of the keyspace in local data center.
func (clusterListener *ClusterListener) GetOrSetCluster(keyspace string, clusterSize int, replicationFactor int) *topology.Cluster {
	clusterListener.Lock()
	t, ok := clusterListener.clusters[keyspaceName(keyspace)]
	if !ok {
		t = topology.NewCluster(keyspace, clusterListener.dataCenter, clusterSize, replicationFactor)
		clusterListener.clusters[keyspaceName(keyspace)] = t
	}
	if clusterSize > 0 {
		t.SetExpectedSize(clusterSize)
	}
	if replicationFactor > 0 {
		t.SetReplicationFactor(replicationFactor)
	}
	clusterListener.Unlock()
	return t
}

// StartListener keeps the listener connected to the master.
func (clusterListener *ClusterListener) StartListener(ctx context.Context, master, dataCenter string) {

	clientMessageChan := make(chan *pb.ClientMessage)

	go util.RetryForever(ctx, clusterListener.clientName+" cluster listener", func() error {
		return clusterListener.registerClientAtMasterServer(master, dataCenter, clientMessageChan)
	}, 2*time.Second)

	go func() {
		for {
			select {
			case msg := <-clientMessageChan:
				clusterListener.processClientMessage(msg)
			}
		}
	}()

	// println("client is connected to master", master, "data center", dataCenter)

	return

}

// SetUnixSocket whether or not use unix socket if available. Default to true.
// When client or gateway is on the same machine as the store server, using unix socket can avoid some network cost.
func (clusterListener *ClusterListener) SetUnixSocket(useUnixSocket bool) {
	clusterListener.disableUnixSocket = !useUnixSocket
}

// HasConnectedKeyspace checks whether the listener has the information of the keyspace or not.
func (clusterListener *ClusterListener) HasConnectedKeyspace(keyspace string) bool {
	cluster, found := clusterListener.GetCluster(keyspace)
	if !found {
		// println("not found cluster")
		return false
	}
	if cluster.CurrentSize() <= 0 {
		// println("cluster current size", cluster.CurrentSize())
		return false
	}
	if cluster.CurrentSize() != cluster.ExpectedSize() {
		// println("cluster current size", cluster.CurrentSize(), "expected", cluster.ExpectedSize())
		return false
	}
	return true
}

func (clusterListener *ClusterListener) processClientMessage(msg *pb.ClientMessage) {
	if msg.GetCluster() != nil {
		glog.V(4).Infof("%s listener get cluster: %v", clusterListener.clientName, msg.GetCluster())
		cluster := clusterListener.GetOrSetCluster(msg.Cluster.Keyspace, int(msg.Cluster.ExpectedClusterSize), int(msg.Cluster.ReplicationFactor))
		for _, node := range msg.Cluster.Nodes {
			addNode(cluster, node)
			for _, shardEventProcess := range clusterListener.shardEventProcessors {
				shardEventProcess.OnShardCreateEvent(cluster, node.StoreResource, node.ShardInfo)
			}
		}
	} else if msg.GetUpdates() != nil {
		glog.V(4).Infof("%s listener get update: %v", clusterListener.clientName, msg.GetUpdates())
		cluster, found := clusterListener.GetCluster(msg.Updates.Keyspace)
		if !found {
			glog.Errorf("%s no keyspace %s found to update", clusterListener.clientName, msg.Updates.Keyspace)
			return
		}
		for _, node := range msg.Updates.Nodes {
			if msg.Updates.GetIsPromotion() {
				promoteNode(cluster, node)
				for _, shardEventProcess := range clusterListener.shardEventProcessors {
					shardEventProcess.OnShardPromoteEvent(cluster, node.StoreResource, node.ShardInfo)
				}
			} else if msg.Updates.GetIsDelete() {
				clusterListener.removeNode(msg.Updates.Keyspace, node)
				for _, shardEventProcess := range clusterListener.shardEventProcessors {
					if shardEventProcess != nil {
						shardEventProcess.OnShardRemoveEvent(cluster, node.StoreResource, node.ShardInfo)
					}
				}
			} else {
				oldShardInfo := addNode(cluster, node)
				for _, shardEventProcess := range clusterListener.shardEventProcessors {
					if oldShardInfo == nil {
						shardEventProcess.OnShardCreateEvent(cluster, node.StoreResource, node.ShardInfo)
					} else if oldShardInfo.Status.String() != node.ShardInfo.String() {
						shardEventProcess.OnShardUpdateEvent(cluster, node.StoreResource, node.ShardInfo, oldShardInfo)
					}
				}
			}
		}
	} else if msg.GetResize() != nil {
		glog.V(4).Infof("%s listener get resize: %v", clusterListener.clientName, msg.GetResize())
		r, found := clusterListener.GetCluster(msg.Resize.Keyspace)
		if !found {
			glog.Errorf("%s no keyspace %s found to resize", clusterListener.clientName, msg.Resize.Keyspace)
			return
		}
		r.SetExpectedSize(int(msg.Resize.TargetClusterSize))
	} else {
		glog.Errorf("%s unknown message %v", clusterListener.clientName, msg)
	}
}
