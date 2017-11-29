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
	return "[keyspaces|datacenters|<keyspace> <data center>]"
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

		descResponse, err := c.masterClient.Describe(
			context.Background(),
			&pb.DescribeRequest{
				DescKeyspaces: &pb.DescribeRequest_DescKeyspaces{},
			},
		)

		if err != nil {
			return err
		}

		keyspaces := descResponse.DescKeyspaces.Keyspaces
		for _, keyspace := range keyspaces {
			fmt.Fprintf(out, "keyspace %v\n", keyspace.Keyspace)
			for _, cluster := range keyspace.Clusters {
				fmt.Fprintf(out, "    cluster %v expected size %d\n",
					cluster.DataCenter, cluster.ExpectedClusterSize)
				for _, node := range cluster.Nodes {
					fmt.Fprintf(out, "        * %v %v\n", node.ShardId, node.Address)
				}
			}
		}

	} else if param == "datacenters" {

		descResponse, err := c.masterClient.Describe(
			context.Background(),
			&pb.DescribeRequest{
				DescDataCenters: &pb.DescribeRequest_DescDataCenters{},
			},
		)

		if err != nil {
			return err
		}

		dataCenters := descResponse.DescDataCenters.DataCenters
		for _, dataCenter := range dataCenters {
			fmt.Fprintf(out, "datacenter %v\n", dataCenter.DataCenter)
			for _, server := range dataCenter.StoreResources {
				fmt.Fprintf(out, "    server %v\n", server.Address)
				for keyspace, storeStatus := range server.StoreStatusesInCluster {
					fmt.Fprintf(out, "        * %v %v\n", keyspace, storeStatus.String())
				}
			}
		}

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
		printCluster(out, descResponse.DescCluster.GetCluster())

	}

	return nil
}

func printCluster(out io.Writer, cluster *pb.Cluster) {
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
