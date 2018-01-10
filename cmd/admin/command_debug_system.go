package admin

import (
	"io"
	"github.com/chrislusf/vasto/pb"
	"context"
)

func init() {
	commands = append(commands, &CommandDebug{})
}

type CommandDebug struct {
	masterClient pb.VastoMasterClient
}

func (c *CommandDebug) Name() string {
	return "debug"
}

func (c *CommandDebug) Help() string {
	return "// print out debug info from master and store consoles"
}

func (c *CommandDebug) SetMasterCilent(masterClient pb.VastoMasterClient) {
	c.masterClient = masterClient
}

func (c *CommandDebug) Do(args []string, out io.Writer) (err error) {

	_, err = c.masterClient.DebugMaster(
		context.Background(),
		&pb.Empty{},
	)

	if err != nil {
		return err
	}

	return nil
}
