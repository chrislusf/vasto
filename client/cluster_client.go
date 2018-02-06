package client

import (
	"github.com/chrislusf/vasto/topology/cluster_listener"
	"github.com/chrislusf/vasto/topology"
	"fmt"
)

type ClusterClient struct {
	Master          string
	DataCenter      string
	keyspace        string
	ClusterListener *cluster_listener.ClusterListener
}

func (c *ClusterClient) GetCluster() (*topology.Cluster, error) {
	cluster, found := c.ClusterListener.GetCluster(c.keyspace)
	if !found {
		return nil, fmt.Errorf("no keyspace %s", c.keyspace)
	}
	return cluster, nil
}
