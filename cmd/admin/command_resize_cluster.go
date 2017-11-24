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
	return "dataCenter newClusterSize"
}

func (c *CommandResizeCluster) SetMasterCilent(masterClient pb.VastoMasterClient) {
	c.masterClient = masterClient
}

func (c *CommandResizeCluster) Do(args []string, out io.Writer) (err error) {

	dc := "defaultDataCenter"
	if len(args) > 0 {
		dc = args[0]
	} else {
		return InvalidArguments
	}

	var clusterSize uint64
	if len(args) > 1 {
		clusterSize, err = strconv.ParseUint(args[1], 10, 32)
		if err != nil {
			return InvalidArguments
		}
	} else {
		return InvalidArguments
	}

	stream, err := c.masterClient.ResizeCluster(
		context.Background(),
		&pb.ResizeRequest{
			DataCenter:  dc,
			ClusterSize: uint32(clusterSize),
		},
	)

	for {
		resizeProgress, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("resize cluster error: %v", err)
		}
		if resizeProgress.Error != "" {
			return fmt.Errorf("Resize Error: %v", resizeProgress.Error)
		}
		if resizeProgress.Progress != "" {
			fmt.Fprintf(out, "Resize: %v\n", resizeProgress.Progress)
		}
	}

	return nil
}
