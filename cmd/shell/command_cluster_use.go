package shell

import (
	"github.com/chrislusf/vasto/vs"
	"io"
)

func init() {
	commands = append(commands, &CommandClusterUse{})
}

type CommandClusterUse struct {
}

func (c *CommandClusterUse) Name() string {
	return "use"
}

func (c *CommandClusterUse) Help() string {
	return "keyspace <keysapce>"
}

func (c *CommandClusterUse) Do(vastoClient *vs.VastoClient, args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	if len(args) != 2 {
		return InvalidArguments
	}

	commandEnv.keyspace = args[1]

	if commandEnv.keyspace != "" {
		commandEnv.clusterClient = vastoClient.NewClusterClient(commandEnv.keyspace)
	}

	return nil
}
