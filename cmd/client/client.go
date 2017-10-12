package client

import (
	"fmt"
	"time"

	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/topology"
	"github.com/chrislusf/vasto/util"
)

type ClientOption struct {
	Master     *string
	DataCenter *string
}

type VastoClient struct {
	option *ClientOption
	// these may need to be protected by atomic
	ring               topology.Ring
	currentClusterSize uint32
	nextClusterSize    uint32
}

func New(option *ClientOption) *VastoClient {
	c := &VastoClient{
		option: option,
		ring:   topology.NewHashRing(*option.DataCenter),
	}
	return c
}

func (c *VastoClient) Start() error {
	clientMessageChan := make(chan *pb.ClientMessage)

	go util.RetryForever(func() error {
		return c.registerClientAtMasterServer(clientMessageChan)
	}, 2*time.Second)

	for {
		select {
		case msg := <-clientMessageChan:
			if msg.GetUpdates() != nil {
				for _, store := range msg.Updates.Stores {
					node := topology.NewNodeFromStore(store)
					if msg.Updates.GetIsDelete() {
						fmt.Printf("remove node %d: %v\n", node.GetId(), node.GetHost())
						c.ring.Remove(node)
					} else {
						fmt.Printf("   add node %d: %v\n", node.GetId(), node.GetHost())
						c.ring.Add(node)
					}
				}
			} else if msg.GetResize() != nil {
				c.currentClusterSize = msg.Resize.CurrentClusterSize
				c.nextClusterSize = msg.Resize.NewClusterSize
				if c.nextClusterSize == 0 {
					fmt.Printf("resized to %d\n", c.currentClusterSize)
				} else {
					fmt.Printf("resizing %d => %d\n", c.currentClusterSize, c.nextClusterSize)
				}
			} else {
				fmt.Printf("unknown message %v\n", msg)
			}
		}
	}

	return nil
}
