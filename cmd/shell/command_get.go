package shell

import (
	"fmt"
	"io"

	"github.com/chrislusf/vasto/client"
)

func init() {
	commands = append(commands, &CommandGet{})
}

type CommandGet struct {
	client *client.VastoClient
}

func (c *CommandGet) Name() string {
	return "get"
}

func (c *CommandGet) Help() string {
	return "key"
}

func (c *CommandGet) SetCilent(client *client.VastoClient) {
	c.client = client
}

func (c *CommandGet) Do(args []string, env map[string]string, writer io.Writer) error {
	options, err := parseEnv(env)
	if err != nil {
		return err
	}

	// fmt.Printf("env: %+v\n", env)
	if len(args) == 1 {
		key := []byte(args[0])

		value, err := c.client.Get(*c.client.Option.Keyspace, key, options...)

		if err == nil {
			fmt.Fprintf(writer, "%s\n", string(value))
		}

		return err
	} else {
		var keys [][]byte
		for _, arg := range args {
			keys = append(keys, []byte(arg))
		}
		keyValues, err := c.client.BatchGet(*c.client.Option.Keyspace, keys, options...)
		if err != nil {
			return err
		}
		for _, keyValue := range keyValues {
			if keyValue == nil {
				continue
			}
			fmt.Fprintf(writer, "%s : %s\n", string(keyValue.Key), string(keyValue.Value))
		}
		return nil
	}
}
