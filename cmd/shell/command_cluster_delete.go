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
	return "cluster.delete"
}

func (c *commandDeleteKeyspace) Help() string {
	return "<cluster_name>"
}

func (c *commandDeleteKeyspace) Do(vastoClient *vs.VastoClient, args []string, commandEnv *commandEnv, writer io.Writer) (err error) {

	if len(args) != 1 {
		return errInvalidArguments
	}

	keyspace := args[0]

	return vastoClient.DeleteCluster(keyspace)

}
