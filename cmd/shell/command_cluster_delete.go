package shell

import (
	"github.com/chrislusf/vasto/goclient/vs"
	"io"
)

func init() {
	commands = append(commands, &commandDeleteKeyspace{})
}

type commandDeleteKeyspace struct {
}

func (c *commandDeleteKeyspace) Name() string {
	return "delete"
}

func (c *commandDeleteKeyspace) Help() string {
	return "cluster <keysapce> <datacenter>"
}

func (c *commandDeleteKeyspace) Do(vastoClient *vs.VastoClient, args []string, commandEnv *commandEnv, writer io.Writer) (err error) {

	if len(args) != 3 {
		return errInvalidArguments
	}
	if args[0] != "cluster" {
		return errInvalidArguments
	}

	keyspace, dc := args[1], args[2]

	return vastoClient.DeleteCluster(keyspace, dc)

}
