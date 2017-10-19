package client

import (
	"fmt"
	"time"

	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/topology"
	"github.com/chrislusf/vasto/util"
	"strings"
)

type ClientOption struct {
	FixedCluster *string
	Master       *string
	DataCenter   *string
}

type VastoClient struct {
	option  *ClientOption
	cluster *topology.ClusterRing
}

func New(option *ClientOption) *VastoClient {
	c := &VastoClient{
		option:  option,
		cluster: topology.NewHashRing(*option.DataCenter),
	}
	return c
}

func (c *VastoClient) startWithFixedCluster() {
	servers := strings.Split(*c.option.FixedCluster, ",")
	c.cluster.SetCurrentSize(int(len(servers)))
	for id, networkHostPort := range servers {
		parts := strings.SplitN(networkHostPort, ":", 2)
		store := &pb.StoreResource{
			Id:      int32(id),
			Network: parts[0],
			Address: parts[1],
		}
		c.AddNode(store)
	}
}

func (c *VastoClient) Start(clientReadyChan chan bool) {

	if *c.option.FixedCluster != "" {
		c.startWithFixedCluster()
		clientReadyChan <- true
		return
	}

	var clientReady bool

	clientMessageChan := make(chan *pb.ClientMessage)

	go util.RetryForever(func() error {
		return c.registerClientAtMasterServer(clientMessageChan)
	}, 2*time.Second)

	for {
		select {
		case msg := <-clientMessageChan:
			if msg.GetCluster() != nil {
				c.cluster.SetCurrentSize(int(msg.Cluster.CurrentClusterSize))
				c.cluster.SetNextSize(int(msg.Cluster.NextClusterSize))
				for _, store := range msg.Cluster.Stores {
					c.AddNode(store)
				}
				if !clientReady {
					clientReady = true
					clientReadyChan <- true
				}
			} else if msg.GetUpdates() != nil {
				for _, store := range msg.Updates.Stores {
					if msg.Updates.GetIsDelete() {
						c.RemoveNode(store)
					} else {
						c.AddNode(store)
					}
				}
			} else if msg.GetResize() != nil {
				c.cluster.SetCurrentSize(int(msg.Resize.CurrentClusterSize))
				c.cluster.SetNextSize(int(msg.Resize.NextClusterSize))
				if c.cluster.NextSize() == 0 {
					fmt.Printf("resized to %d\n", c.cluster.CurrentSize())
				} else {
					fmt.Printf("resizing %d => %d\n", c.cluster.CurrentSize(), c.cluster.NextSize())
				}
			} else {
				fmt.Printf("unknown message %v\n", msg)
			}
		}
	}

}
