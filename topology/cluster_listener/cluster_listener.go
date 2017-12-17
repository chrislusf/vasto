package cluster_listener

import (
	"fmt"
	"time"

	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/topology"
	"github.com/chrislusf/vasto/util"
	"strings"
	"sync"
	"context"
	"log"
)

type keyspace_name string

type keyspace_follow_message struct {
	keyspace   keyspace_name
	isUnfollow bool
}
type ClusterListener struct {
	sync.RWMutex
	clusters                  map[keyspace_name]*topology.ClusterRing
	keyspaceFollowMessageChan chan keyspace_follow_message
	dataCenter                string
	shardEventProcessors      []ShardEventProcessor
}

func NewClusterClient(dataCenter string) *ClusterListener {
	return &ClusterListener{
		clusters:                  make(map[keyspace_name]*topology.ClusterRing),
		keyspaceFollowMessageChan: make(chan keyspace_follow_message, 1),
		dataCenter:                dataCenter,
	}
}

func (clusterListener *ClusterListener) AddExistingKeyspace(keyspace string, clusterSize int, replicationFactor int) {
	clusterListener.Lock()
	clusterListener.clusters[keyspace_name(keyspace)] = topology.NewHashRing(keyspace, clusterListener.dataCenter, clusterSize, replicationFactor)
	clusterListener.Unlock()
}

// AddNewKeyspace register to listen to one keyspace
func (clusterListener *ClusterListener) AddNewKeyspace(keyspace string, clusterSize int, replicationFactor int) *topology.ClusterRing {
	t := clusterListener.GetOrSetClusterRing(keyspace, clusterSize, replicationFactor)
	clusterListener.keyspaceFollowMessageChan <- keyspace_follow_message{keyspace: keyspace_name(keyspace)}
	return t
}

func (clusterListener *ClusterListener) RemoveKeyspace(keyspace string) {
	clusterListener.Lock()
	delete(clusterListener.clusters, keyspace_name(keyspace))
	clusterListener.Unlock()
	clusterListener.keyspaceFollowMessageChan <- keyspace_follow_message{keyspace: keyspace_name(keyspace), isUnfollow: true}
}

func (clusterListener *ClusterListener) GetClusterRing(keyspace string) (r *topology.ClusterRing, found bool) {
	clusterListener.RLock()
	r, found = clusterListener.clusters[keyspace_name(keyspace)]
	clusterListener.RUnlock()
	return
}

func (clusterListener *ClusterListener) GetOrSetClusterRing(keyspace string, clusterSize int, replicationFactor int) (*topology.ClusterRing) {
	clusterListener.RLock()
	t, ok := clusterListener.clusters[keyspace_name(keyspace)]
	if !ok {
		t = topology.NewHashRing(keyspace, clusterListener.dataCenter, clusterSize, replicationFactor)
		clusterListener.clusters[keyspace_name(keyspace)] = t
	}
	t.SetExpectedSize(clusterSize)
	t.SetReplicationFactor(replicationFactor)
	clusterListener.RUnlock()
	return t
}

// SetNodes initialize the cluster to a comma-separated node list,
// where each node has the format of network:host:port
// The network is either tcp or socket
func (clusterListener *ClusterListener) SetNodes(keyspace string, fixedCluster string) {
	servers := strings.Split(fixedCluster, ",")
	var nodes []*pb.ClusterNode
	for id, networkHostPort := range servers {
		parts := strings.SplitN(networkHostPort, ":", 2)
		node := &pb.ClusterNode{
			StoreResource: &pb.StoreResource{
				Network: parts[0],
				Address: parts[1],
			},
			ShardInfo: &pb.ShardInfo{
				NodeId:  uint32(id),
				ShardId: uint32(id),
			},
		}
		nodes = append(nodes, node)
	}
	clusterListener.GetOrSetClusterRing(keyspace, len(servers), 1)
	for _, node := range nodes {
		clusterListener.AddNode(keyspace, node)
	}
}

// if master is not empty, return when client is connected to the master and
// fetched the initial cluster information.
func (clusterListener *ClusterListener) StartListener(ctx context.Context, master, dataCenter string, blockUntilConnected bool) {

	if master == "" {
		return
	}

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
					r := clusterListener.GetOrSetClusterRing(msg.Cluster.Keyspace, int(msg.Cluster.ExpectedClusterSize), int(msg.Cluster.ReplicationFactor))
					r.SetNextSize(int(msg.Cluster.NextClusterSize))
					for _, node := range msg.Cluster.Nodes {
						clusterListener.AddNode(msg.Cluster.Keyspace, node)
						for _, shardEventProcess := range clusterListener.shardEventProcessors {
							shardEventProcess.OnShardCreateEvent(r, node.StoreResource, node.ShardInfo)
						}
					}
					if !clientConnected {
						clientConnected = true
						if blockUntilConnected {
							clientConnectedChan <- true
						}
					}
				} else if msg.GetUpdates() != nil {
					r, found := clusterListener.GetClusterRing(msg.Updates.Keyspace)
					if !found {
						log.Printf("no keyspace %s found to update", msg.Updates.Keyspace)
						continue
					}
					for _, node := range msg.Updates.Nodes {
						if msg.Updates.GetIsDelete() {
							clusterListener.RemoveNode(msg.Updates.Keyspace, node)
							for _, shardEventProcess := range clusterListener.shardEventProcessors {
								shardEventProcess.OnShardRemoveEvent(r, node.StoreResource, node.ShardInfo)
							}
						} else {
							oldShardInfo := clusterListener.AddNode(msg.Updates.Keyspace, node)
							for _, shardEventProcess := range clusterListener.shardEventProcessors {
								if oldShardInfo == nil {
									shardEventProcess.OnShardCreateEvent(r, node.StoreResource, node.ShardInfo)
								} else if oldShardInfo.Status.String() != node.ShardInfo.String() {
									shardEventProcess.OnShardUpdateEvent(r, node.StoreResource, node.ShardInfo, oldShardInfo)
								}
							}
						}
					}
				} else if msg.GetResize() != nil {
					r, found := clusterListener.GetClusterRing(msg.Resize.Keyspace)
					if !found {
						log.Printf("no keyspace %s found to resize", msg.Resize.Keyspace)
						continue
					}
					r.SetExpectedSize(int(msg.Resize.CurrentClusterSize))
					r.SetNextSize(int(msg.Resize.NextClusterSize))
					if r.NextSize() == 0 {
						fmt.Printf("keyspace %s dc %s resized to %d\n", msg.Resize.Keyspace, clusterListener.dataCenter, r.CurrentSize())
					} else {
						fmt.Printf("keyspace %s dc %s resizing %d => %d\n", msg.Resize.Keyspace, clusterListener.dataCenter, r.ExpectedSize(), r.NextSize())
					}
				} else {
					fmt.Printf("unknown message %v\n", msg)
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
