package shell

import (
	"fmt"
	"io"

	"github.com/chrislusf/vasto/vs"
)

func init() {
	commands = append(commands, &CommandPut{})
}

type CommandPut struct {
}

func (c *CommandPut) Name() string {
	return "put"
}

func (c *CommandPut) Help() string {
	return "<key> <value>"
}

func (c *CommandPut) Do(vastoClient *vs.VastoClient, args []string, commandEnv *CommandEnv, writer io.Writer) error {
	options, err := parseEnv(commandEnv.env)
	if err != nil {
		return err
	}
	if commandEnv.clusterClient == nil {
		return NoKeyspaceSelected
	}

	if len(args) < 2 {
		return InvalidArguments
	}

	key := []byte(args[0])
	value := []byte(args[1])

	err = commandEnv.clusterClient.Put(vs.Key(key), value, options...)

	fmt.Fprintln(writer)

	return err
}
