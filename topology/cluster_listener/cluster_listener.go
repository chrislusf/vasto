package cluster_listener

import (
	"time"

	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/topology"
	"github.com/chrislusf/vasto/util"
	"sync"
	"context"
	"log"
	"gopkg.in/fatih/pool.v2"
)

type keyspace_name string

type keyspace_follow_message struct {
	keyspace   keyspace_name
	isUnfollow bool
}
type ClusterListener struct {
	sync.RWMutex
	clusters                  map[keyspace_name]*topology.Cluster
	keyspaceFollowMessageChan chan keyspace_follow_message
	dataCenter                string
	shardEventProcessors      []ShardEventProcessor
	verbose                   bool
	clientName                string
	connPools                 map[string]pool.Pool
	connPoolLock              sync.RWMutex
}

func NewClusterClient(dataCenter string, clientName string) *ClusterListener {
	return &ClusterListener{
		clusters:                  make(map[keyspace_name]*topology.Cluster),
		keyspaceFollowMessageChan: make(chan keyspace_follow_message, 1),
		dataCenter:                dataCenter,
		clientName:                clientName,
		connPools:                 make(map[string]pool.Pool),
	}
}

func (clusterListener *ClusterListener) AddExistingKeyspace(keyspace string, clusterSize int, replicationFactor int) {
	clusterListener.Lock()
	clusterListener.clusters[keyspace_name(keyspace)] = topology.NewCluster(keyspace, clusterListener.dataCenter, clusterSize, replicationFactor)
	clusterListener.Unlock()
}

// AddNewKeyspace register to listen to one keyspace
func (clusterListener *ClusterListener) AddNewKeyspace(keyspace string, clusterSize int, replicationFactor int) *topology.Cluster {
	t := clusterListener.GetOrSetCluster(keyspace, clusterSize, replicationFactor)
	clusterListener.keyspaceFollowMessageChan <- keyspace_follow_message{keyspace: keyspace_name(keyspace)}
	return t
}

func (clusterListener *ClusterListener) RemoveKeyspace(keyspace string) {
	clusterListener.Lock()
	if _, found := clusterListener.clusters[keyspace_name(keyspace)]; found {
		delete(clusterListener.clusters, keyspace_name(keyspace))
		clusterListener.keyspaceFollowMessageChan <- keyspace_follow_message{keyspace: keyspace_name(keyspace), isUnfollow: true}
	}
	clusterListener.Unlock()
}

func (clusterListener *ClusterListener) GetCluster(keyspace string) (r *topology.Cluster, found bool) {
	clusterListener.RLock()
	r, found = clusterListener.clusters[keyspace_name(keyspace)]
	clusterListener.RUnlock()
	return
}

func (clusterListener *ClusterListener) GetOrSetCluster(keyspace string, clusterSize int, replicationFactor int) (*topology.Cluster) {
	clusterListener.RLock()
	t, ok := clusterListener.clusters[keyspace_name(keyspace)]
	if !ok {
		t = topology.NewCluster(keyspace, clusterListener.dataCenter, clusterSize, replicationFactor)
		clusterListener.clusters[keyspace_name(keyspace)] = t
	}
	t.SetExpectedSize(clusterSize)
	t.SetReplicationFactor(replicationFactor)
	clusterListener.RUnlock()
	return t
}

// StartListener keeps the listener connected to the master.
// if blockUntilConnected, return when client is connected to the master and has fetched the initial cluster information.
func (clusterListener *ClusterListener) StartListener(ctx context.Context, master, dataCenter string, blockUntilConnected bool) {

	var clientConnected bool
	var clientConnectedChan chan bool
	if blockUntilConnected {
		clientConnectedChan = make(chan bool, 1)
	}

	clientMessageChan := make(chan *pb.ClientMessage)

	go util.RetryForever(ctx, "cluster listner to master", func() error {
		return clusterListener.registerClientAtMasterServer(master, dataCenter, clientMessageChan)
	}, 2*time.Second)

	go func() {
		for {
			select {
			case msg := <-clientMessageChan:
				if msg.GetCluster() != nil {
					// log.Printf("listener get cluster: %v", msg.GetCluster())
					cluster := clusterListener.GetOrSetCluster(msg.Cluster.Keyspace, int(msg.Cluster.ExpectedClusterSize), int(msg.Cluster.ReplicationFactor))
					for _, node := range msg.Cluster.Nodes {
						AddNode(cluster, node)
						for _, shardEventProcess := range clusterListener.shardEventProcessors {
							shardEventProcess.OnShardCreateEvent(cluster, node.StoreResource, node.ShardInfo)
						}
					}
					if !clientConnected {
						clientConnected = true
						if blockUntilConnected {
							clientConnectedChan <- true
						}
					}
				} else if msg.GetUpdates() != nil {
					log.Printf("listener get update: %v", msg.GetUpdates())
					cluster, found := clusterListener.GetCluster(msg.Updates.Keyspace)
					if !found {
						log.Printf("no keyspace %s found to update", msg.Updates.Keyspace)
						continue
					}
					for _, node := range msg.Updates.Nodes {
						if msg.Updates.GetIsPromotion() {
							PromoteNode(cluster, node)
							for _, shardEventProcess := range clusterListener.shardEventProcessors {
								shardEventProcess.OnShardPromoteEvent(cluster, node.StoreResource, node.ShardInfo)
							}
						} else if msg.Updates.GetIsDelete() {
							clusterListener.RemoveNode(msg.Updates.Keyspace, node)
							for _, shardEventProcess := range clusterListener.shardEventProcessors {
								if shardEventProcess != nil {
									shardEventProcess.OnShardRemoveEvent(cluster, node.StoreResource, node.ShardInfo)
								}
							}
						} else {
							oldShardInfo := AddNode(cluster, node)
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
					log.Printf("listener get resize: %v", msg.GetResize())
					r, found := clusterListener.GetCluster(msg.Resize.Keyspace)
					if !found {
						log.Printf("no keyspace %s found to resize", msg.Resize.Keyspace)
						continue
					}
					r.SetExpectedSize(int(msg.Resize.TargetClusterSize))
				} else {
					log.Printf("unknown message %v", msg)
				}
			}
		}
	}()

	if blockUntilConnected {
		<-clientConnectedChan
		close(clientConnectedChan)
	}

	// println("client is connected to master", master, "data center", dataCenter)

	return

}

func (clusterListener *ClusterListener) SetVerboseLog(verbose bool) {
	clusterListener.verbose = verbose
}
