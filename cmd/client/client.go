package client

import (
	"github.com/chrislusf/vasto/topology/cluster_listener"
)

type ClientOption struct {
	FixedCluster *string
	Master       *string
	DataCenter   *string
}

type VastoClient struct {
	option          *ClientOption
	ClusterListener *cluster_listener.ClusterListener
}

func New(option *ClientOption) *VastoClient {
	c := &VastoClient{
		option:          option,
		ClusterListener: cluster_listener.NewClusterClient(*option.DataCenter),
	}
	return c
}

func (c *VastoClient) startWithFixedCluster() {
	c.ClusterListener.SetNodes(*c.option.FixedCluster)
}

func (c *VastoClient) Start() {

	if *c.option.FixedCluster != "" {
		c.startWithFixedCluster()
		return
	}

	c.ClusterListener.Start(*c.option.Master, *c.option.DataCenter)

}
