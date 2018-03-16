package shell

import (
	"fmt"
	"io"

	"github.com/chrislusf/vasto/goclient/vs"
)

func init() {
	commands = append(commands, &commandPut{})
}

type commandPut struct {
}

func (c *commandPut) Name() string {
	return "put"
}

func (c *commandPut) Help() string {
	return "<key> <value>"
}

func (c *commandPut) Do(vastoClient *vs.VastoClient, args []string, commandEnv *commandEnv, writer io.Writer) error {
	if commandEnv.clusterClient == nil {
		return errNoKeyspaceSelected
	}

	if len(args) < 2 {
		return errInvalidArguments
	}

	key := []byte(args[0])
	value := []byte(args[1])

	err := commandEnv.clusterClient.Put(vs.Key(key), value)

	fmt.Fprintln(writer)

	return err
}
