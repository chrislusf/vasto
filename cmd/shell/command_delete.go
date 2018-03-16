package shell

import (
	"io"

	"github.com/chrislusf/vasto/goclient/vs"
)

func init() {
	commands = append(commands, &commandDelete{})
}

type commandDelete struct {
}

func (c *commandDelete) Name() string {
	return "del"
}

func (c *commandDelete) Help() string {
	return "<key>"
}

func (c *commandDelete) Do(vastoClient *vs.VastoClient, args []string, commandEnv *commandEnv, writer io.Writer) error {
	if commandEnv.clusterClient == nil {
		return errNoKeyspaceSelected
	}

	key := vs.Key([]byte(args[0]))

	err := commandEnv.clusterClient.Delete(key)

	return err
}
