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
	return "cluster <keysapce>"
}

func (c *commandCompactCluster) Do(vastoClient *vs.VastoClient, args []string, commandEnv *commandEnv, writer io.Writer) error {
	if len(args) != 2 {
		return errInvalidArguments
	}
	if args[0] != "cluster" {
		return errInvalidArguments
	}

	keyspace := args[1]

	return vastoClient.CompactCluster(keyspace)
}
