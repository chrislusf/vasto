package shell

import (
	"io"
	"github.com/chrislusf/vasto/client"
)

func init() {
	commands = append(commands, &CommandDeleteKeyspace{})
}

type CommandDeleteKeyspace struct {
}

func (c *CommandDeleteKeyspace) Name() string {
	return "delete"
}

func (c *CommandDeleteKeyspace) Help() string {
	return "cluster <keysapce> <datacenter>"
}

func (c *CommandDeleteKeyspace) Do(vastoClient *client.VastoClient, args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	if len(args) != 3 {
		return InvalidArguments
	}
	if args[0] != "cluster" {
		return InvalidArguments
	}

	keyspace, dc := args[1], args[2]

	return vastoClient.DeleteCluster(keyspace, dc)

}
