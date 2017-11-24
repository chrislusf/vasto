package cluster_listener

import (
	"fmt"
	"time"

	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/topology"
	"github.com/chrislusf/vasto/util"
	"strings"
)

type ClusterListener struct {
	topology.ClusterRing
}

func NewClusterClient(dataCenter string) *ClusterListener {
	return &ClusterListener{
		ClusterRing: topology.NewHashRing(dataCenter),
	}
}

// SetNodes initialize the cluster to a comma-separated node list,
// where each node has the format of network:host:port
// The network is either tcp or socket
func (l *ClusterListener) SetNodes(fixedCluster string) {
	servers := strings.Split(fixedCluster, ",")
	var nodes []*pb.StoreResource
	for id, networkHostPort := range servers {
		parts := strings.SplitN(networkHostPort, ":", 2)
		store := &pb.StoreResource{
			Id:      int32(id),
			Network: parts[0],
			Address: parts[1],
		}
		nodes = append(nodes, store)
	}
	l.SetExpectedSize(len(nodes))
	for _, node := range nodes {
		l.AddNode(node)
	}
}

// if master is not empty, return when client is connected to the master and
// fetched the initial cluster information.
func (l *ClusterListener) Start(master, dataCenter string) {

	if master == "" {
		return
	}

	var clientConnected bool
	clientConnectedChan := make(chan bool, 1)

	clientMessageChan := make(chan *pb.ClientMessage)

	go util.RetryForever(func() error {
		return l.registerClientAtMasterServer(master, dataCenter, clientMessageChan)
	}, 2*time.Second)

	go func() {
		for {
			select {
			case msg := <-clientMessageChan:
				if msg.GetCluster() != nil {
					l.SetExpectedSize(int(msg.Cluster.ExpectedClusterSize))
					l.SetNextSize(int(msg.Cluster.NextClusterSize))
					for _, store := range msg.Cluster.Stores {
						l.AddNode(store)
					}
					if !clientConnected {
						clientConnected = true
						clientConnectedChan <- true
					}
				} else if msg.GetUpdates() != nil {
					for _, store := range msg.Updates.Stores {
						if msg.Updates.GetIsDelete() {
							l.RemoveNode(store)
						} else {
							l.AddNode(store)
						}
					}
				} else if msg.GetResize() != nil {
					l.SetExpectedSize(int(msg.Resize.CurrentClusterSize))
					l.SetNextSize(int(msg.Resize.NextClusterSize))
					if l.NextSize() == 0 {
						fmt.Printf("resized to %d\n", l.CurrentSize())
					} else {
						fmt.Printf("resizing %d => %d\n", l.ExpectedSize(), l.NextSize())
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
