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
	return "compact"
}

func (c *commandCompactCluster) Help() string {
	return "cluster <keysapce> <datacenter>"
}

func (c *commandCompactCluster) Do(vastoClient *vs.VastoClient, args []string, commandEnv *commandEnv, writer io.Writer) error {
	if len(args) != 3 {
		return errInvalidArguments
	}
	if args[0] != "cluster" {
		return errInvalidArguments
	}

	keyspace, dc := args[1], args[2]

	return vastoClient.CompactCluster(keyspace, dc)
}
