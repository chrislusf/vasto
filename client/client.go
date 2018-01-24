package client

import (
	"context"
	"github.com/chrislusf/vasto/topology/cluster_listener"
	"time"
	"google.golang.org/grpc"
	"log"
	"github.com/chrislusf/vasto/pb"
	"fmt"
)

type VastoClient struct {
	ctx             context.Context
	Master          string
	DataCenter      string
	ClientName      string
	ClusterListener *cluster_listener.ClusterListener
	masterClient    pb.VastoMasterClient
}

func NewClient(ctx context.Context, clientName, master, dataCenter string) *VastoClient {
	c := &VastoClient{
		ctx:             ctx,
		ClusterListener: cluster_listener.NewClusterClient(dataCenter, clientName),
		Master:          master,
		ClientName:      clientName,
		DataCenter:      dataCenter,
	}
	c.ClusterListener.RegisterShardEventProcessor(&cluster_listener.ClusterEventLogger{Prefix: "[" + clientName + "]"})
	c.ClusterListener.StartListener(ctx, c.Master, c.DataCenter)

	conn, err := grpc.Dial(c.Master, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("%s fail to dial %v: %v", c.ClientName, c.Master, err)
	}
	c.masterClient = pb.NewVastoMasterClient(conn)

	return c
}

func (c *VastoClient) RegisterForKeyspace(keyspace string) {
	c.ClusterListener.AddNewKeyspace(keyspace, 0, 0)
	for !c.ClusterListener.HasConnectedKeyspace(keyspace) {
		time.Sleep(100 * time.Millisecond)
	}
}

func (c *VastoClient) CreateKeyspace(keyspace string, clusterSize, replicationFactor int) error {

	log.Printf("%s from %s sending request to %v to create keyspace:%v size:%v replicationFactor:%v", c.ClientName, c.DataCenter, c.Master, keyspace, clusterSize, replicationFactor)
	resp, err := c.masterClient.CreateCluster(
		c.ctx,
		&pb.CreateClusterRequest{
			DataCenter:        c.DataCenter,
			Keyspace:          keyspace,
			ClusterSize:       uint32(clusterSize),
			ReplicationFactor: uint32(replicationFactor),
		},
	)

	if err != nil {
		return fmt.Errorf("%s create cluster request: %v", c.ClientName, err)
	}
	if resp.Error != "" {
		return fmt.Errorf("%s create cluster: %v", c.ClientName, resp.Error)
	}

	return nil

}
