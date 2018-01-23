package admin

import (
	"context"
	"fmt"
	"github.com/chrislusf/vasto/pb"
	"io"
)

func init() {
	commands = append(commands, &CommandDeleteKeyspace{})
}

type CommandDeleteKeyspace struct {
	masterClient pb.VastoMasterClient
}

func (c *CommandDeleteKeyspace) Name() string {
	return "delete"
}

func (c *CommandDeleteKeyspace) Help() string {
	return "cluster <keysapce> <datacenter>"
}

func (c *CommandDeleteKeyspace) SetMasterCilent(masterClient pb.VastoMasterClient) {
	c.masterClient = masterClient
}

func (c *CommandDeleteKeyspace) Do(args []string, out io.Writer) (err error) {

	if len(args) != 3 {
		return InvalidArguments
	}
	if args[0] != "cluster" {
		return InvalidArguments
	}

	keyspace, dc := args[1], args[2]

	resp, err := c.masterClient.DeleteCluster(
		context.Background(),
		&pb.DeleteClusterRequest{
			DataCenter: dc,
			Keyspace:   keyspace,
		},
	)

	if err != nil {
		return fmt.Errorf("delete cluster request: %v", err)
	}
	if resp.Error != "" {
		return fmt.Errorf("delete cluster: %v", resp.Error)
	}

	return nil
}
