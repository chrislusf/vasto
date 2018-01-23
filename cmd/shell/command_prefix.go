package shell

import (
	"fmt"
	"io"
	"strconv"

	"github.com/chrislusf/vasto/client"
)

func init() {
	commands = append(commands, &CommandPrefix{})
}

type CommandPrefix struct {
}

func (c *CommandPrefix) Name() string {
	return "prefix"
}

func (c *CommandPrefix) Help() string {
	return "prefix [limit lastSeenKey], prefix should also be the partition key"
}

func (c *CommandPrefix) Do(vastoClient *client.VastoClient, args []string, commandEnv *CommandEnv, writer io.Writer) error {
	options, err := parseEnv(commandEnv.env)
	if err != nil {
		return err
	}

	prefix := []byte(args[0])
	limit := uint32(100)
	var lastSeenKey []byte
	if len(args) >= 2 {
		t, err := strconv.ParseUint(args[1], 10, 32)
		if err != nil {
			return err
		}
		limit = uint32(t)
	}
	if len(args) >= 3 {
		lastSeenKey = []byte(args[2])
	}

	keyValues, err := vastoClient.GetByPrefix(commandEnv.keyspace, nil, prefix, limit, lastSeenKey, options...)

	if err != nil {
		return err
	}
	for _, keyValue := range keyValues {
		fmt.Fprintf(writer, "%s : %s\n", string(keyValue.Key), string(keyValue.Value))
	}
	return nil
}
