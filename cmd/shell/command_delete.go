package shell

import (
	"io"

	. "github.com/chrislusf/vasto/vs"
)

func init() {
	commands = append(commands, &CommandDelete{})
}

type CommandDelete struct {
}

func (c *CommandDelete) Name() string {
	return "del"
}

func (c *CommandDelete) Help() string {
	return "<key>"
}

func (c *CommandDelete) Do(vastoClient *VastoClient, args []string, commandEnv *CommandEnv, writer io.Writer) error {
	if commandEnv.clusterClient == nil {
		return NoKeyspaceSelected
	}

	key := Key([]byte(args[0]))

	err := commandEnv.clusterClient.Delete(key)

	return err
}
