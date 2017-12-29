package admin

import (
	"context"
	"fmt"
	"io"
	"strconv"

	"github.com/chrislusf/vasto/pb"
)

func init() {
	commands = append(commands, &CommandResizeCluster{})
}

type CommandResizeCluster struct {
	masterClient pb.VastoMasterClient
}

func (c *CommandResizeCluster) Name() string {
	return "resize"
}

func (c *CommandResizeCluster) Help() string {
	return "<keyspace> <data_center> <new_cluster_size>"
}

func (c *CommandResizeCluster) SetMasterCilent(masterClient pb.VastoMasterClient) {
	c.masterClient = masterClient
}

func (c *CommandResizeCluster) Do(args []string, out io.Writer) (err error) {

	if len(args) != 3 {
		return InvalidArguments
	}
	keyspace := args[0]
	dc := args[1]
	newClusterSize, err := strconv.ParseUint(args[2], 10, 32)
	if err != nil {
		return InvalidArguments
	}

	resp, err := c.masterClient.ResizeCluster(
		context.Background(),
		&pb.ResizeRequest{
			Keyspace:    keyspace,
			DataCenter:  dc,
			ClusterSize: uint32(newClusterSize),
		},
	)

	if err != nil {
		return fmt.Errorf("resize request: %v", err)
	}
	if resp.Error != "" {
		return fmt.Errorf("resize: %v", resp.Error)
	}

	return nil
}
