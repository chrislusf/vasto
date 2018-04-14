package shell

import (
	"fmt"
	"io"
	"strconv"

	"github.com/chrislusf/vasto/goclient/vs"
	"github.com/chrislusf/vasto/pb"
)

func init() {
	commands = append(commands, &commandCreateKeyspace{})
}

type commandCreateKeyspace struct {
}

func (c *commandCreateKeyspace) Name() string {
	return "create"
}

func (c *commandCreateKeyspace) Help() string {
	return "cluster <keysapce> <datacenter> <server count> <replication factor>"
}

func (c *commandCreateKeyspace) Do(vastoClient *vs.VastoClient, args []string, commandEnv *commandEnv, writer io.Writer) (err error) {

	if len(args) != 5 {
		return errInvalidArguments
	}
	if args[0] != "cluster" {
		return errInvalidArguments
	}

	keyspace, dc := args[1], args[2]

	clusterSize, err := strconv.ParseUint(args[3], 10, 64)
	if err != nil {
		println("can not parse server count", args[3])
		return errInvalidArguments
	}
	replicationFactor, err := strconv.ParseUint(args[4], 10, 64)
	if err != nil {
		println("can not parse replication factor", args[4])
		return errInvalidArguments
	}

	if replicationFactor == 0 {
		println("replication factor", replicationFactor, "should be greater than 0")
		return errInvalidArguments
	}

	if replicationFactor > clusterSize {
		println("replication factor", replicationFactor, "should not be bigger than cluster size", clusterSize)
		return errInvalidArguments
	}

	cluster, err := vastoClient.CreateCluster(keyspace, dc, int(clusterSize), int(replicationFactor))

	if err != nil {
		return fmt.Errorf("create cluster request: %v", err)
	}

	printCluster(writer, cluster)

	return nil
}

func printCluster(out io.Writer, cluster *pb.Cluster) {
	if cluster != nil {
		fmt.Fprintf(out, "Cluster Expected Size: %d\n", cluster.ExpectedClusterSize)
		fmt.Fprintf(out, "Cluster Current  Size: %d\n", cluster.CurrentClusterSize)

		for _, node := range cluster.Nodes {
			fmt.Fprintf(out, "        * shard %v server %v %v\n",
				node.ShardInfo.ShardId, node.ShardInfo.ServerId, node.StoreResource.Address)
		}

	}
}
