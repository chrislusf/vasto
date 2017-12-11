package client

import (
	"github.com/chrislusf/vasto/topology/cluster_listener"
	"context"
)

type ClientOption struct {
	FixedCluster *string
	Master       *string
	DataCenter   *string
	Keyspace     *string
}

type VastoClient struct {
	Option          *ClientOption
	ClusterListener *cluster_listener.ClusterListener
}

func NewClient(option *ClientOption) *VastoClient {
	c := &VastoClient{
		Option:          option,
		ClusterListener: cluster_listener.NewClusterClient(*option.DataCenter),
	}
	return c
}

func (c *VastoClient) startWithFixedCluster() {
	c.ClusterListener.SetNodes(*c.Option.Keyspace, *c.Option.FixedCluster)
}

func (c *VastoClient) StartClient(ctx context.Context) {

	if *c.Option.FixedCluster != "" {
		c.startWithFixedCluster()
		return
	}

	c.ClusterListener.AddExistingKeyspace(*c.Option.Keyspace, 0, 0)
	c.ClusterListener.StartListener(ctx, *c.Option.Master, *c.Option.DataCenter, true)
}
