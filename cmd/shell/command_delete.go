package shell

import (
	"io"

	. "github.com/chrislusf/vasto/client"
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
	return "key"
}

func (c *CommandDelete) Do(vastoClient *VastoClient, args []string, commandEnv *CommandEnv, writer io.Writer) error {
	options, err := parseEnv(commandEnv.env)
	if err != nil {
		return err
	}

	if commandEnv.clusterClient == nil {
		return NoKeyspaceSelected
	}

	key := Key([]byte(args[0]))

	err = commandEnv.clusterClient.Delete(key, options...)

	return err
}
