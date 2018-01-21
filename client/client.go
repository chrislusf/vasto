package client

import (
	"github.com/chrislusf/vasto/topology/cluster_listener"
	"context"
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
}

func NewClient(option *ClientOption) *VastoClient {
	c := &VastoClient{
		Option:          option,
		ClusterListener: cluster_listener.NewClusterClient(*option.DataCenter, option.ClientName),
	}
	return c
}

func (c *VastoClient) StartClient(ctx context.Context) {
	c.ClusterListener.AddExistingKeyspace(*c.Option.Keyspace, 0, 0)
	c.ClusterListener.StartListener(ctx, *c.Option.Master, *c.Option.DataCenter, true)
}
