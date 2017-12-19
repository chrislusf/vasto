package admin

import (
	"context"
	"fmt"
	"io"
	"strconv"

	"github.com/chrislusf/vasto/pb"
)

func init() {
	commands = append(commands, &CommandClusterReplaceNode{})
}

type CommandClusterReplaceNode struct {
	masterClient pb.VastoMasterClient
}

func (c *CommandClusterReplaceNode) Name() string {
	return "replace"
}

func (c *CommandClusterReplaceNode) Help() string {
	return "<keyspace> <data_center> <node_id> <new_server_ip:new_server_por>"
}

func (c *CommandClusterReplaceNode) SetMasterCilent(masterClient pb.VastoMasterClient) {
	c.masterClient = masterClient
}

func (c *CommandClusterReplaceNode) Do(args []string, out io.Writer) (err error) {

	if len(args) != 4 {
		return InvalidArguments
	}
	dc := args[0]
	keyspace := args[1]
	nodeId, err := strconv.ParseUint(args[2], 10, 32)
	if err != nil {
		return InvalidArguments
	}
	address := args[3]

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
