package client

import (
	"fmt"
	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/util"
	"time"
)

type ClientOption struct {
	Master     *string
	DataCenter *string
}

type VastoClient struct {
	option *ClientOption
}

func New(option *ClientOption) *VastoClient {
	c := &VastoClient{
		option: option,
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
			fmt.Printf("received message %v\n", msg)
		}
	}

	return nil
}
