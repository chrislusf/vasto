package client

import (
	"github.com/chrislusf/vasto/topology/cluster_listener"
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

func New(option *ClientOption) *VastoClient {
	c := &VastoClient{
		Option:          option,
		ClusterListener: cluster_listener.NewClusterClient(*option.DataCenter),
	}
	c.ClusterListener.ListenFor(*option.Keyspace)
	return c
}

func (c *VastoClient) startWithFixedCluster() {
	c.ClusterListener.SetNodes(*c.Option.Keyspace, *c.Option.FixedCluster)
}

func (c *VastoClient) Start() {

	if *c.Option.FixedCluster != "" {
		c.startWithFixedCluster()
		return
	}

	c.ClusterListener.Start(*c.Option.Master, *c.Option.Keyspace, *c.Option.DataCenter)

}
