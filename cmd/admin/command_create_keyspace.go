package admin

import (
	"context"
	"fmt"
	"io"
	"strconv"

	"github.com/chrislusf/vasto/pb"
)

func init() {
	commands = append(commands, &CommandCreateKeyspace{})
}

type CommandCreateKeyspace struct {
	masterClient pb.VastoMasterClient
}

func (c *CommandCreateKeyspace) Name() string {
	return "create"
}

func (c *CommandCreateKeyspace) Help() string {
	return "cluster <keysapce> <datacenter> <server count> <replication factor>"
}

func (c *CommandCreateKeyspace) SetMasterCilent(masterClient pb.VastoMasterClient) {
	c.masterClient = masterClient
}

func (c *CommandCreateKeyspace) Do(args []string, out io.Writer) (err error) {

	if len(args) != 5 {
		return InvalidArguments
	}
	if args[0] != "cluster" {
		return InvalidArguments
	}

	keyspace, dc := args[1], args[2]

	clusterSize, err := strconv.ParseUint(args[3], 10, 64)
	if err != nil {
		println("can not parse server count", args[3])
		return InvalidArguments
	}
	replicationFactor, err := strconv.ParseUint(args[4], 10, 64)
	if err != nil {
		println("can not parse replication factor", args[4])
		return InvalidArguments
	}

	if replicationFactor == 0 {
		println("replication factor", replicationFactor, "should be greater than 0")
		return InvalidArguments
	}

	if replicationFactor > clusterSize {
		println("replication factor", replicationFactor, "should not be bigger than cluster size", clusterSize)
		return InvalidArguments
	}

	resp, err := c.masterClient.CreateCluster(
		context.Background(),
		&pb.CreateClusterRequest{
			DataCenter:        dc,
			Keyspace:          keyspace,
			ClusterSize:       uint32(clusterSize),
			ReplicationFactor: uint32(replicationFactor),
		},
	)

	if err != nil {
		return fmt.Errorf("create cluster request: %v", err)
	}
	if resp.Error != "" {
		return fmt.Errorf("create cluster: %v", resp.Error)
	}

	printCluster(out, resp.Cluster)

	return nil
}
