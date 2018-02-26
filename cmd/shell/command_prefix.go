package shell

import (
	"fmt"
	"io"
	"strconv"

	"github.com/chrislusf/vasto/vs"
)

func init() {
	commands = append(commands, &commandPrefix{})
}

type commandPrefix struct {
}

func (c *commandPrefix) Name() string {
	return "prefix"
}

func (c *commandPrefix) Help() string {
	return "<prefix> [<limit> <lastSeenKey>], prefix should also be the partition key"
}

func (c *commandPrefix) Do(vastoClient *vs.VastoClient, args []string, commandEnv *commandEnv, writer io.Writer) error {

	if commandEnv.clusterClient == nil {
		return errNoKeyspaceSelected
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
