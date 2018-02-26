package admin

import (
	"context"
	"github.com/chrislusf/vasto/pb"
	"io"
)

func init() {
	commands = append(commands, &commandDebug{})
}

type commandDebug struct {
	masterClient pb.VastoMasterClient
}

func (c *commandDebug) Name() string {
	return "debug"
}

func (c *commandDebug) Help() string {
	return "// print out debug info from master and store consoles"
}

func (c *commandDebug) SetMasterCilent(masterClient pb.VastoMasterClient) {
	c.masterClient = masterClient
}

func (c *commandDebug) Do(args []string, out io.Writer) (err error) {

	_, err = c.masterClient.DebugMaster(
		context.Background(),
		&pb.Empty{},
	)

	if err != nil {
		return err
	}

	return nil
}
