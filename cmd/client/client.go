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
	ring   topology.Ring
}

func New(option *ClientOption) *VastoClient {
	c := &VastoClient{
		option: option,
		ring:   topology.NewHashRing(*option.DataCenter),
	}
	return c
}

func (c *VastoClient) Start() error {
	msgChan := make(chan *pb.ClientMessage)

	go util.RetryForever(func() error {
		return c.registerClientAtMasterServer(msgChan)
	}, 2*time.Second)

	for {
		select {
		case msg := <-msgChan:
			for _, store := range msg.Stores {
				node := topology.NewNodeFromStore(store)
				if msg.GetIsDelete() {
					c.ring.Remove(node)
				} else {
					c.ring.Add(node)
				}
			}
			fmt.Printf("received message %v\n", msg)
		}
	}

	return nil
}
