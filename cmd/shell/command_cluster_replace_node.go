package shell

import (
	"io"
	"strconv"

	"github.com/chrislusf/vasto/client"
)

func init() {
	commands = append(commands, &CommandClusterReplaceNode{})
}

type CommandClusterReplaceNode struct {
}

func (c *CommandClusterReplaceNode) Name() string {
	return "replace"
}

func (c *CommandClusterReplaceNode) Help() string {
	return "<keyspace> <data_center> <node_id> <new_server_ip:new_server_por>"
}

func (c *CommandClusterReplaceNode) Do(vastoClient *client.VastoClient, args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	if len(args) != 4 {
		return InvalidArguments
	}
	keyspace := args[0]
	dc := args[1]
	nodeId, err := strconv.ParseUint(args[2], 10, 32)
	if err != nil {
		return InvalidArguments
	}
	newAddress := args[3]

	return vastoClient.ReplaceNode(keyspace, dc, uint32(nodeId), newAddress)

}
