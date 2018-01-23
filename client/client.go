package client

import (
	"context"
	"github.com/chrislusf/vasto/topology/cluster_listener"
	"time"
)

type VastoClient struct {
	ClusterListener *cluster_listener.ClusterListener
	Master          string
	DataCenter      string
	ClientName      string
}

func NewClient(ctx context.Context, clientName, master, dataCenter string) *VastoClient {
	c := &VastoClient{
		ClusterListener: cluster_listener.NewClusterClient(dataCenter, clientName),
		Master:          master,
		ClientName:      clientName,
		DataCenter:      dataCenter,
	}
	c.ClusterListener.StartListener(ctx, c.Master, c.DataCenter)
	return c
}

func (c *VastoClient) RegisterForKeyspace(keyspace string) {
	c.ClusterListener.AddExistingKeyspace(keyspace, 0, 0)
	for !c.ClusterListener.HasConnectedKeyspace(keyspace) {
		time.Sleep(100 * time.Millisecond)
	}
}
