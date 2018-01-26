package shell

import (
	"io"
	"strconv"

	"github.com/chrislusf/vasto/client"
)

func init() {
	commands = append(commands, &CommandResizeCluster{})
}

type CommandResizeCluster struct {
}

func (c *CommandResizeCluster) Name() string {
	return "resize"
}

func (c *CommandResizeCluster) Help() string {
	return "<keyspace> <data_center> <new_cluster_size>"
}

func (c *CommandResizeCluster) Do(vastoClient *client.VastoClient, args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	if len(args) != 3 {
		return InvalidArguments
	}
	keyspace, dc := args[0], args[1]
	newClusterSize, err := strconv.ParseUint(args[2], 10, 32)
	if err != nil {
		return InvalidArguments
	}

	return vastoClient.ResizeCluster(keyspace, dc, int(newClusterSize))
}
