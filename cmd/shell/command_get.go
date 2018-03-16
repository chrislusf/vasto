package shell

import (
	"fmt"
	"io"

	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/util"
	"github.com/chrislusf/vasto/goclient/vs"
)

func init() {
	commands = append(commands, &commandGet{})
}

type commandGet struct {
}

func (c *commandGet) Name() string {
	return "get"
}

func (c *commandGet) Help() string {
	return "<key>"
}

func (c *commandGet) Do(vastoClient *vs.VastoClient, args []string, commandEnv *commandEnv, writer io.Writer) error {
	if commandEnv.clusterClient == nil {
		return errNoKeyspaceSelected
	}

	// fmt.Printf("env: %+v\n", env)
	if len(args) == 1 {
		key := vs.Key([]byte(args[0]))

		value, dt, err := commandEnv.clusterClient.Get(key)

		if err == nil {
			switch dt {
			case pb.OpAndDataType_FLOAT64:
				fmt.Fprintf(writer, "%f\n", util.BytesToFloat64(value))
			default:
				fmt.Fprintf(writer, "%s\n", string(value))
			}
		}

		return err
	}

	var keys []*vs.KeyObject
	for _, arg := range args {
		keys = append(keys, vs.Key([]byte(arg)))
	}
	keyValues, err := commandEnv.clusterClient.BatchGet(keys)
	if err != nil {
		return err
	}
	for _, keyValue := range keyValues {
		if keyValue == nil {
			continue
		}
		fmt.Fprintf(writer, "%s : %s\n", string(keyValue.GetKey()), string(keyValue.GetValue()))
	}
	return nil
}
