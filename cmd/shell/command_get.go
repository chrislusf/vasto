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
}

func (c *CommandGet) Name() string {
	return "get"
}

func (c *CommandGet) Help() string {
	return "key"
}

func (c *CommandGet) Do(vastoClient *client.VastoClient, args []string, commandEnv *CommandEnv, writer io.Writer) error {
	options, err := parseEnv(commandEnv.env)
	if err != nil {
		return err
	}
	if commandEnv.clusterClient == nil {
		return NoKeyspaceSelected
	}

	// fmt.Printf("env: %+v\n", env)
	if len(args) == 1 {
		key := client.Key([]byte(args[0]))

		value, err := commandEnv.clusterClient.Get(key, options...)

		if err == nil {
			fmt.Fprintf(writer, "%s\n", string(value))
		}

		return err
	} else {
		var keys []*client.KeyObject
		for _, arg := range args {
			keys = append(keys, client.Key([]byte(arg)))
		}
		keyValues, err := commandEnv.clusterClient.BatchGet(keys, options...)
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
