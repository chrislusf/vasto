package shell

import (
	"fmt"
	"io"
	"strconv"

	"github.com/chrislusf/vasto/vs"
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
	return "<prefix> [<limit> <lastSeenKey>], prefix should also be the partition key"
}

func (c *CommandPrefix) Do(vastoClient *vs.VastoClient, args []string, commandEnv *CommandEnv, writer io.Writer) error {

	if commandEnv.clusterClient == nil {
		return NoKeyspaceSelected
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

	keyValues, err := commandEnv.clusterClient.GetByPrefix(nil, prefix, limit, lastSeenKey)

	if err != nil {
		return err
	}
	for _, keyValue := range keyValues {
		fmt.Fprintf(writer, "%s : %s\n", string(keyValue.GetKey()), string(keyValue.GetValue()))
	}
	return nil
}
