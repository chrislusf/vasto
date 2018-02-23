package shell

import (
	"context"
	"fmt"
	"io"

	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/vs"
)

func init() {
	commands = append(commands, &CommandDesc{})
}

type CommandDesc struct {
}

func (c *CommandDesc) Name() string {
	return "desc"
}

func (c *CommandDesc) Help() string {
	return "keyspaces|datacenters|<keyspace> <data center>"
}

func (c *CommandDesc) Do(vastoClient *vs.VastoClient, args []string, commandEnv *CommandEnv, out io.Writer) error {

	param := "keyspaces"
	if len(args) > 0 {
		param = args[0]
	}
	if param == "keyspaces" {

		descResponse, err := vastoClient.MasterClient.Describe(
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
			fmt.Fprintf(out, "keyspace %v client:%d\n", keyspace.Keyspace, keyspace.ClientCount)
			for _, cluster := range keyspace.Clusters {
				fmt.Fprintf(out, "    cluster %v expected size %d\n",
					cluster.DataCenter, cluster.ExpectedClusterSize)
				for _, node := range cluster.Nodes {
					fmt.Fprintf(out, "        * node %v shard %v %v\n",
						node.ShardInfo.ServerId, node.ShardInfo.ShardId, node.StoreResource.Address)
				}
			}
		}

	} else if param == "datacenters" {

		descResponse, err := vastoClient.MasterClient.Describe(
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
			fmt.Fprintf(out, "datacenter %v client:%d\n", dataCenter.DataCenter, dataCenter.ClientCount)
			for _, server := range dataCenter.StoreResources {
				fmt.Fprintf(out, "    server %v total:%d GB, allocated:%d GB, Tags:%s\n",
					server.Address, server.DiskSizeGb, server.AllocatedSizeGb, server.Tags)
			}
		}

	} else if len(args) == 2 {

		descResponse, err := vastoClient.MasterClient.Describe(
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

		if descResponse.DescCluster == nil {
			return fmt.Errorf("no cluster keyspace(%v) dc(%v) found", param, args[1])
		}

		fmt.Fprintf(out, "Cluster Client Count : %d\n", descResponse.DescCluster.ClientCount)
		printCluster(out, descResponse.DescCluster.GetCluster())
		if descResponse.DescCluster.GetNextCluster() != nil {
			nextCluster := descResponse.DescCluster.GetNextCluster()
			fmt.Fprintf(out, "=> Cluster Size: %d\n", nextCluster.ExpectedClusterSize)
			if nextCluster.ExpectedClusterSize >= descResponse.DescCluster.GetCluster().ExpectedClusterSize {
				for _, node := range nextCluster.Nodes {
					if node.StoreResource.Address == "" {
						continue
					}
					fmt.Fprintf(out, "        + node %v shard %v %v\n",
						node.ShardInfo.ServerId, node.ShardInfo.ShardId, node.StoreResource.Address)
				}
			}
		}

	}

	return nil
}
