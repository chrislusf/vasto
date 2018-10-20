package shell

import (
	"io"
	"strconv"

	"github.com/chrislusf/vasto/goclient/vs"
)

func init() {
	commands = append(commands, &commandResizeCluster{})
}

type commandResizeCluster struct {
}

func (c *commandResizeCluster) Name() string {
	return "resize"
}

func (c *commandResizeCluster) Help() string {
	return "<keyspace> <new_cluster_size>"
}

func (c *commandResizeCluster) Do(vastoClient *vs.VastoClient, args []string, commandEnv *commandEnv, writer io.Writer) (err error) {

	if len(args) != 2 {
		return errInvalidArguments
	}
	keyspace := args[0]
	newClusterSize, err := strconv.ParseUint(args[1], 10, 32)
	if err != nil {
		return errInvalidArguments
	}

	return vastoClient.ResizeCluster(keyspace, int(newClusterSize))
}
