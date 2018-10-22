package shell

import (
	"io"

	"github.com/chrislusf/vasto/goclient/vs"
)

func init() {
	commands = append(commands, &commandCompactCluster{})
}

type commandCompactCluster struct {
}

func (c *commandCompactCluster) Name() string {
	return "cluster.compact"
}

func (c *commandCompactCluster) Help() string {
	return "<cluster_name>"
}

func (c *commandCompactCluster) Do(vastoClient *vs.VastoClient, args []string, commandEnv *commandEnv, writer io.Writer) error {
	if len(args) != 1 {
		return errInvalidArguments
	}

	keyspace := args[0]

	return vastoClient.CompactCluster(keyspace)
}
