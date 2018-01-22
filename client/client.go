package client

import (
	"github.com/chrislusf/vasto/topology/cluster_listener"
	"context"
	"time"
)

type ClientOption struct {
	Master     *string
	DataCenter *string
	Keyspace   *string
	ClientName string
}

type VastoClient struct {
	Option          *ClientOption
	ClusterListener *cluster_listener.ClusterListener
	Master          string
	DataCenter      string
	ClientName      string
}

func NewClient2(option *ClientOption) *VastoClient {
	c := &VastoClient{
		Option:          option,
		ClusterListener: cluster_listener.NewClusterClient(*option.DataCenter, option.ClientName),
		Master:          *option.Master,
		ClientName:      option.ClientName,
		DataCenter:      *option.DataCenter,
	}
	return c
}

func NewClient(clientName, master, dataCenter string) *VastoClient {
	c := &VastoClient{
		ClusterListener: cluster_listener.NewClusterClient(dataCenter, clientName),
		Master:          master,
		ClientName:      clientName,
		DataCenter:      dataCenter,
	}
	return c
}

func (c *VastoClient) StartClient(ctx context.Context) {
	c.ClusterListener.StartListener(ctx, c.Master, c.DataCenter)
	c.ClusterListener.AddExistingKeyspace(*c.Option.Keyspace, 0, 0)
	for !c.ClusterListener.HasConnectedKeyspace(*c.Option.Keyspace) {
		time.Sleep(100 * time.Millisecond)
	}
}
