package vs

import (
	"context"
	"fmt"
	"github.com/chrislusf/glog"
	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/topology/cluster_listener"
	"google.golang.org/grpc"
	"sync"
	"time"
)

type VastoClient struct {
	ctx                context.Context
	Master             string
	DataCenter         string
	ClientName         string
	ClusterListener    *cluster_listener.ClusterListener
	MasterClient       pb.VastoMasterClient
	clusterClients     map[string]*ClusterClient
	clusterClientsLock sync.Mutex
}

func NewClient(ctx context.Context, clientName, master, dataCenter string) *VastoClient {
	c := &VastoClient{
		ctx:             ctx,
		ClusterListener: cluster_listener.NewClusterClient(dataCenter, clientName),
		Master:          master,
		ClientName:      clientName,
		DataCenter:      dataCenter,
		clusterClients:  make(map[string]*ClusterClient),
	}
	// c.ClusterListener.RegisterShardEventProcessor(&cluster_listener.ClusterEventLogger{Prefix: clientName + " "})
	c.ClusterListener.StartListener(ctx, c.Master, c.DataCenter)

	conn, err := grpc.Dial(c.Master, grpc.WithInsecure())
	if err != nil {
		glog.Fatalf("%s fail to dial %v: %v", c.ClientName, c.Master, err)
	}
	c.MasterClient = pb.NewVastoMasterClient(conn)

	return c
}

func (c *VastoClient) GetClusterClient(keyspace string) (clusterClient *ClusterClient) {
	c.ClusterListener.AddNewKeyspace(keyspace, 0, 0)
	for !c.ClusterListener.HasConnectedKeyspace(keyspace) {
		time.Sleep(100 * time.Millisecond)
	}

	var found bool
	c.clusterClientsLock.Lock()
	clusterClient, found = c.clusterClients[keyspace]
	if !found {
		clusterClient = &ClusterClient{
			Master:          c.Master,
			DataCenter:      c.DataCenter,
			keyspace:        keyspace,
			ClusterListener: c.ClusterListener,
		}
		c.clusterClients[keyspace] = clusterClient
	}
	c.clusterClientsLock.Unlock()

	return
}

func (c *VastoClient) CreateCluster(keyspace, dataCenter string, clusterSize, replicationFactor int) (*pb.Cluster, error) {

	if replicationFactor == 0 {
		return nil, fmt.Errorf("replication factor %d should be greater than 0", replicationFactor)
	}

	if replicationFactor > clusterSize {
		return nil, fmt.Errorf("replication factor %d should not be bigger than cluster size %d", replicationFactor, clusterSize)
	}

	resp, err := c.MasterClient.CreateCluster(
		c.ctx,
		&pb.CreateClusterRequest{
			DataCenter:        dataCenter,
			Keyspace:          keyspace,
			ClusterSize:       uint32(clusterSize),
			ReplicationFactor: uint32(replicationFactor),
		},
	)

	if err != nil {
		return nil, fmt.Errorf("%s create cluster request: %v", c.ClientName, err)
	}
	if resp.Error != "" {
		return nil, fmt.Errorf("%s create cluster: %v", c.ClientName, resp.Error)
	}

	return resp.Cluster, nil

}

func (c *VastoClient) DeleteCluster(keyspace, dataCenter string) error {

	resp, err := c.MasterClient.DeleteCluster(
		c.ctx,
		&pb.DeleteClusterRequest{
			DataCenter: dataCenter,
			Keyspace:   keyspace,
		},
	)

	if err != nil {
		return fmt.Errorf("delete cluster request: %v", err)
	}
	if resp.Error != "" {
		return fmt.Errorf("delete cluster: %v", resp.Error)
	}

	return nil

}

func (c *VastoClient) ResizeCluster(keyspace, dataCenter string, newClusterSize int) error {

	resp, err := c.MasterClient.ResizeCluster(
		c.ctx,
		&pb.ResizeRequest{
			DataCenter:        dataCenter,
			Keyspace:          keyspace,
			TargetClusterSize: uint32(newClusterSize),
		},
	)

	if err != nil {
		return fmt.Errorf("resize request: %v", err)
	}
	if resp.Error != "" {
		return fmt.Errorf("resize: %v", resp.Error)
	}

	return nil

}

func (c *VastoClient) ReplaceNode(keyspace, dataCenter string, nodeId uint32, newAddress string) error {

	resp, err := c.MasterClient.ReplaceNode(
		c.ctx,
		&pb.ReplaceNodeRequest{
			DataCenter: dataCenter,
			Keyspace:   keyspace,
			NodeId:     uint32(nodeId),
			NewAddress: newAddress,
		},
	)

	if err != nil {
		return fmt.Errorf("replace node request: %v", err)
	}
	if resp.Error != "" {
		return fmt.Errorf("replace node: %v", resp.Error)
	}

	return nil

}
