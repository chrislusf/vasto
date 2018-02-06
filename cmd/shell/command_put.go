package shell

import (
	"fmt"
	"io"

	"github.com/chrislusf/vasto/client"
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
	return "key value"
}

func (c *CommandPut) Do(vastoClient *client.VastoClient, args []string, commandEnv *CommandEnv, writer io.Writer) error {
	options, err := parseEnv(commandEnv.env)
	if err != nil {
		return err
	}
	if len(args) < 2 {
		return InvalidArguments
	}

	key := []byte(args[0])
	value := []byte(args[1])

	row := client.NewRow(key, value)

	err = commandEnv.clusterClient.Put([]*client.Row{row}, options...)

	fmt.Fprintln(writer)

	return err
}
