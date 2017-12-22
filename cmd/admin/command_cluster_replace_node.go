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
	keyspace := args[0]
	dc := args[1]
	nodeId, err := strconv.ParseUint(args[2], 10, 32)
	if err != nil {
		return InvalidArguments
	}
	address := args[3]

	resp, err := c.masterClient.ReplaceNodePrepare(
		context.Background(),
		&pb.ReplaceNodePrepareRequest{
			DataCenter: dc,
			Keyspace:   keyspace,
			NodeId:     uint32(nodeId),
			NewAddress: address,
		},
	)

	if err != nil {
		return fmt.Errorf("replace node request: %v", err)
	}
	if resp.Error != "" {
		return fmt.Errorf("replace node: %v", resp.Error)
	}

	return nil
}
