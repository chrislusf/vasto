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
	return "use"
}

func (c *commandClusterUse) Help() string {
	return "keyspace <keysapce>"
}

func (c *commandClusterUse) Do(vastoClient *vs.VastoClient, args []string, commandEnv *commandEnv, writer io.Writer) (err error) {

	if len(args) != 2 {
		return errInvalidArguments
	}

	commandEnv.keyspace = args[1]

	if commandEnv.keyspace != "" {
		commandEnv.clusterClient = vastoClient.NewClusterClient(commandEnv.keyspace)
	}

	return nil
}
