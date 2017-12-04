package cluster_listener

import (
	"fmt"
	"time"

	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/topology"
	"github.com/chrislusf/vasto/util"
	"strings"
	"sync"
)

type keyspace_name string
type ClusterListener struct {
	sync.RWMutex
	clusters             map[keyspace_name]*topology.ClusterRing
	keyspaceChan         chan string
	dataCenter           string
	shardEventProcessors []ShardEventProcessor
}

func NewClusterClient(dataCenter string) *ClusterListener {
	return &ClusterListener{
		clusters:     make(map[keyspace_name]*topology.ClusterRing),
		keyspaceChan: make(chan string, 1),
		dataCenter:   dataCenter,
	}
}

func (clusterListener *ClusterListener) AddExistingKeyspace(keyspace string, clusterSize int) {
	clusterListener.Lock()
	clusterListener.clusters[keyspace_name(keyspace)] = topology.NewHashRing(keyspace, clusterListener.dataCenter, clusterSize)
	clusterListener.Unlock()
}

// AddNewKeyspace register to listen to one keyspace
func (clusterListener *ClusterListener) AddNewKeyspace(keyspace string, clusterSize int) {
	clusterListener.Lock()
	clusterListener.clusters[keyspace_name(keyspace)] = topology.NewHashRing(keyspace, clusterListener.dataCenter, clusterSize)
	clusterListener.Unlock()
	println("listen for keyspace", keyspace)
	clusterListener.keyspaceChan <- keyspace
}

func (clusterListener *ClusterListener) GetClusterRing(keyspace string) *topology.ClusterRing {
	clusterListener.RLock()
	t := clusterListener.clusters[keyspace_name(keyspace)]
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
			ShardStatus: &pb.ShardStatus{
				NodeId:  uint32(id),
				ShardId: uint32(id),
			},
		}
		nodes = append(nodes, node)
	}
	r := clusterListener.GetClusterRing(keyspace)
	r.SetExpectedSize(len(nodes))
	for _, node := range nodes {
		clusterListener.AddNode(keyspace, node)
	}
}

// if master is not empty, return when client is connected to the master and
// fetched the initial cluster information.
func (clusterListener *ClusterListener) StartListener(master, dataCenter string, blockUntilConnected bool) {

	if master == "" {
		return
	}

	var clientConnected bool
	var clientConnectedChan chan bool
	if blockUntilConnected {
		clientConnectedChan = make(chan bool, 1)
	}

	clientMessageChan := make(chan *pb.ClientMessage)

	go util.RetryForever(func() error {
		return clusterListener.registerClientAtMasterServer(master, dataCenter, clientMessageChan)
	}, 2*time.Second)

	go func() {
		for {
			select {
			case msg := <-clientMessageChan:
				if msg.GetCluster() != nil {
					r := clusterListener.GetClusterRing(msg.Cluster.Keyspace)
					r.SetExpectedSize(int(msg.Cluster.ExpectedClusterSize))
					r.SetNextSize(int(msg.Cluster.NextClusterSize))
					for _, node := range msg.Cluster.Nodes {
						clusterListener.AddNode(msg.Cluster.Keyspace, node)
						for _, shardEventProcess := range clusterListener.shardEventProcessors {
							shardEventProcess.OnShardCreateEvent(r, node.StoreResource, node.ShardStatus)
						}
					}
					if !clientConnected {
						clientConnected = true
						if blockUntilConnected {
							clientConnectedChan <- true
						}
					}
				} else if msg.GetUpdates() != nil {
					r := clusterListener.GetClusterRing(msg.Updates.Keyspace)
					for _, node := range msg.Updates.Nodes {
						if msg.Updates.GetIsDelete() {
							clusterListener.RemoveNode(msg.Updates.Keyspace, node)
							for _, shardEventProcess := range clusterListener.shardEventProcessors {
								shardEventProcess.OnShardRemoveEvent(r, node.StoreResource, node.ShardStatus)
							}
						} else {
							oldShardStatus := clusterListener.AddNode(msg.Updates.Keyspace, node)
							for _, shardEventProcess := range clusterListener.shardEventProcessors {
								if oldShardStatus == nil {
									shardEventProcess.OnShardCreateEvent(r, node.StoreResource, node.ShardStatus)
								} else if oldShardStatus.Status.String() != node.ShardStatus.String() {
									shardEventProcess.OnShardUpdateEvent(r, node.StoreResource, node.ShardStatus, oldShardStatus)
								}
							}
						}
					}
				} else if msg.GetResize() != nil {
					r := clusterListener.GetClusterRing(msg.Resize.Keyspace)
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
