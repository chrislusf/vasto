package vs

import (
	"context"
	"fmt"
	"github.com/chrislusf/glog"
	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/topology/clusterlistener"
	"google.golang.org/grpc"
	"time"
)

// VastoClient communicates with master to manage and listen to clusters topology changes.
type VastoClient struct {
	ctx             context.Context
	Master          string
	ClientName      string
	ClusterListener *clusterlistener.ClusterListener
	MasterClient    pb.VastoMasterClient
}

// NewVastoClient creates a vasto client which contains a listener for the vasto system topology changes
func NewVastoClient(ctx context.Context, clientName, master string) *VastoClient {
	c := &VastoClient{
		ctx:             ctx,
		ClusterListener: clusterlistener.NewClusterListener(clientName),
		Master:          master,
		ClientName:      clientName,
	}
	// c.ClusterListener.RegisterShardEventProcessor(&clusterlistener.ClusterEventLogger{Prefix: clientName + " "})
	c.ClusterListener.StartListener(ctx, c.Master)

	conn, err := grpc.Dial(c.Master, grpc.WithInsecure())
	if err != nil {
		glog.Fatalf("%s fail to dial %v: %v", c.ClientName, c.Master, err)
	}
	c.MasterClient = pb.NewVastoMasterClient(conn)

	return c
}

// NewClusterClient create a lightweight client to access a specific cluster
// in a specific data center. The call will block if the keyspace is not created in this data center.
func (c *VastoClient) NewClusterClient(keyspace string) (clusterClient *ClusterClient) {
	c.ClusterListener.AddNewKeyspace(keyspace, 0, 0)
	for !c.ClusterListener.HasConnectedKeyspace(keyspace) {
		time.Sleep(100 * time.Millisecond)
	}

	return &ClusterClient{
		keyspace:        keyspace,
		ClusterListener: c.ClusterListener,
	}

}

// CreateCluster creates a new cluster of the keyspace in the data center, with size and replication factor
func (c *VastoClient) CreateCluster(keyspace string, clusterSize, replicationFactor int) (*pb.Cluster, error) {

	if replicationFactor == 0 {
		return nil, fmt.Errorf("replication factor %d should be greater than 0", replicationFactor)
	}

	if replicationFactor > clusterSize {
		return nil, fmt.Errorf("replication factor %d should not be bigger than cluster size %d", replicationFactor, clusterSize)
	}

	resp, err := c.MasterClient.CreateCluster(
		c.ctx,
		&pb.CreateClusterRequest{
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

// DeleteCluster deletes the cluster of the keyspace in the data center
func (c *VastoClient) DeleteCluster(keyspace string) error {

	resp, err := c.MasterClient.DeleteCluster(
		c.ctx,
		&pb.DeleteClusterRequest{
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

// CompactCluster deletes the cluster of the keyspace in the data center
func (c *VastoClient) CompactCluster(keyspace string) error {

	resp, err := c.MasterClient.CompactCluster(
		c.ctx,
		&pb.CompactClusterRequest{
			Keyspace:   keyspace,
		},
	)

	if err != nil {
		return fmt.Errorf("compact cluster request: %v", err)
	}
	if resp.Error != "" {
		return fmt.Errorf("compact cluster: %v", resp.Error)
	}

	return nil

}

// ResizeCluster changes the size of the cluster of the keyspace and data center
func (c *VastoClient) ResizeCluster(keyspace string, newClusterSize int) error {

	resp, err := c.MasterClient.ResizeCluster(
		c.ctx,
		&pb.ResizeRequest{
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

// ReplaceNode replaces one server in the cluster of the keyspace and data center.
func (c *VastoClient) ReplaceNode(keyspace string, nodeId uint32, newAddress string) error {

	resp, err := c.MasterClient.ReplaceNode(
		c.ctx,
		&pb.ReplaceNodeRequest{
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
