package shell

import (
	"fmt"
	"io"

	"github.com/chrislusf/vasto/cmd/client"
)

func init() {
	commands = append(commands, &CommandPut{})
}

type CommandPut struct {
	client *client.VastoClient
}

func (c *CommandPut) Name() string {
	return "put"
}

func (c *CommandPut) Help() string {
	return "key value"
}

func (c *CommandPut) SetCilent(client *client.VastoClient) {
	c.client = client
}

func (c *CommandPut) Do(args []string, env map[string]string, writer io.Writer) error {
	options, err := parseEnv(env)
	if err != nil {
		return err
	}
	if len(args) < 2 {
		return InvalidArguments
	}

	key := []byte(args[0])
	value := []byte(args[1])

	err = c.client.Put(*c.client.Option.Keyspace, nil, key, value, options...)

	fmt.Fprintln(writer)

	return err
}
