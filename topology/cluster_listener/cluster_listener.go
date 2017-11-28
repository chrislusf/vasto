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
	clusters   map[keyspace_name]*topology.ClusterRing
	dataCenter string
}

func NewClusterClient(dataCenter string) *ClusterListener {
	return &ClusterListener{
		clusters:   make(map[keyspace_name]*topology.ClusterRing),
		dataCenter: dataCenter,
	}
}

func (l *ClusterListener) ListenFor(keyspace string) {
	l.Lock()
	l.clusters[keyspace_name(keyspace)] = topology.NewHashRing(keyspace, l.dataCenter)
	l.Unlock()
}

func (l *ClusterListener) GetClusterRing(keyspace string) *topology.ClusterRing {
	l.Lock()
	t := l.clusters[keyspace_name(keyspace)]
	l.Unlock()
	return t
}

// SetNodes initialize the cluster to a comma-separated node list,
// where each node has the format of network:host:port
// The network is either tcp or socket
func (l *ClusterListener) SetNodes(keyspace string, fixedCluster string) {
	servers := strings.Split(fixedCluster, ",")
	var nodes []*pb.ClusterNode
	for id, networkHostPort := range servers {
		parts := strings.SplitN(networkHostPort, ":", 2)
		node := &pb.ClusterNode{
			ShardId: uint32(id),
			Network: parts[0],
			Address: parts[1],
		}
		nodes = append(nodes, node)
	}
	r := l.GetClusterRing(keyspace)
	r.SetExpectedSize(len(nodes))
	for _, node := range nodes {
		l.AddNode(keyspace, node)
	}
}

// if master is not empty, return when client is connected to the master and
// fetched the initial cluster information.
func (l *ClusterListener) Start(master, keyspace, dataCenter string) {

	if master == "" {
		return
	}

	var clientConnected bool
	clientConnectedChan := make(chan bool, 1)

	clientMessageChan := make(chan *pb.ClientMessage)

	go util.RetryForever(func() error {
		return l.registerClientAtMasterServer(master, keyspace, dataCenter, clientMessageChan)
	}, 2*time.Second)

	go func() {
		for {
			select {
			case msg := <-clientMessageChan:
				if msg.GetCluster() != nil {
					r := l.GetClusterRing(msg.Cluster.Keyspace)
					r.SetExpectedSize(int(msg.Cluster.ExpectedClusterSize))
					r.SetNextSize(int(msg.Cluster.NextClusterSize))
					for _, store := range msg.Cluster.Nodes {
						l.AddNode(msg.Cluster.Keyspace, store)
					}
					if !clientConnected {
						clientConnected = true
						clientConnectedChan <- true
					}
				} else if msg.GetUpdates() != nil {
					for _, store := range msg.Updates.Nodes {
						if msg.Updates.GetIsDelete() {
							l.RemoveNode(msg.Updates.Keyspace, store)
						} else {
							l.AddNode(msg.Updates.Keyspace, store)
						}
					}
				} else if msg.GetResize() != nil {
					r := l.GetClusterRing(msg.Resize.Keyspace)
					r.SetExpectedSize(int(msg.Resize.CurrentClusterSize))
					r.SetNextSize(int(msg.Resize.NextClusterSize))
					if r.NextSize() == 0 {
						fmt.Printf("keyspace %s dc %s resized to %d\n", msg.Resize.Keyspace, l.dataCenter, r.CurrentSize())
					} else {
						fmt.Printf("keyspace %s dc %s resizing %d => %d\n", msg.Resize.Keyspace, l.dataCenter, r.ExpectedSize(), r.NextSize())
					}
				} else {
					fmt.Printf("unknown message %v\n", msg)
				}
			}
		}
	}()

	<-clientConnectedChan
	close(clientConnectedChan)

	// println("client is connected to master", master, "data center", dataCenter)

	return

}
