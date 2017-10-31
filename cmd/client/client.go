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
	clusterListener *cluster_listener.ClusterListener
}

func New(option *ClientOption) *VastoClient {
	c := &VastoClient{
		option:          option,
		clusterListener: cluster_listener.NewClusterClient(*option.DataCenter),
	}
	return c
}

func (c *VastoClient) startWithFixedCluster() {
	c.clusterListener.SetNodes(*c.option.FixedCluster)
}

func (c *VastoClient) Start() {

	if *c.option.FixedCluster != "" {
		c.startWithFixedCluster()
		return
	}

	c.clusterListener.Start(*c.option.Master, *c.option.DataCenter)

}
