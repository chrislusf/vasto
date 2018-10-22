package shell

import (
	"github.com/chrislusf/vasto/goclient/vs"
	"io"
)

func init() {
	commands = append(commands, &commandClusterUse{})
}

type commandClusterUse struct {
}

func (c *commandClusterUse) Name() string {
	return "cluster.use"
}

func (c *commandClusterUse) Help() string {
	return "<cluster_name>"
}

func (c *commandClusterUse) Do(vastoClient *vs.VastoClient, args []string, commandEnv *commandEnv, writer io.Writer) (err error) {

	if len(args) != 1 {
		return errInvalidArguments
	}

	commandEnv.keyspace = args[0]

	if commandEnv.keyspace != "" {
		commandEnv.clusterClient = vastoClient.NewClusterClient(commandEnv.keyspace)
	}

	return nil
}
