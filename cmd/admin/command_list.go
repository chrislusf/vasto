package admin

import (
	"context"
	"fmt"
	"io"

	"github.com/chrislusf/vasto/pb"
)

func init() {
	commands = append(commands, &CommandDesc{})
}

type CommandDesc struct {
	masterClient pb.VastoMasterClient
}

func (c *CommandDesc) Name() string {
	return "desc"
}

func (c *CommandDesc) Help() string {
	return "[keyspaces|data_centers|<keyspace> <data center>]"
}

func (c *CommandDesc) SetMasterCilent(masterClient pb.VastoMasterClient) {
	c.masterClient = masterClient
}

func (c *CommandDesc) Do(args []string, out io.Writer) error {

	param := "keyspaces"
	if len(args) > 0 {
		param = args[0]
	}
	if param == "keyspaces" {

	} else if param == "data_centers" {

	} else if len(args) == 2 {
		descResponse, err := c.masterClient.Describe(
			context.Background(),
			&pb.DescribeRequest{
				DescCluster: &pb.DescribeRequest_DescCluster{
					Keyspace:   param,
					DataCenter: args[1],
				},
			},
		)

		if err != nil {
			return err
		}

		fmt.Fprintf(out, "Cluster Client Count : %d\n", descResponse.ClientCount)
		cluster := descResponse.GetCluster()
		if cluster != nil {
			fmt.Fprintf(out, "Cluster Expected Size: %d\n", cluster.ExpectedClusterSize)
			fmt.Fprintf(out, "Cluster Current  Size: %d\n", cluster.CurrentClusterSize)
			if cluster.NextClusterSize != 0 {
				fmt.Fprintf(out, "Cluster is changing to: %d\n", cluster.NextClusterSize)
			}

			for _, node := range cluster.Nodes {
				fmt.Fprintf(out, "%4d: %32v\n", node.GetShardId(), node.GetAddress())
			}
		}

	}

	return nil
}
