package shell

import (
	"io"
	"strconv"

	"github.com/chrislusf/vasto/vs"
)

func init() {
	commands = append(commands, &commandClusterReplaceNode{})
}

type commandClusterReplaceNode struct {
}

func (c *commandClusterReplaceNode) Name() string {
	return "replace"
}

func (c *commandClusterReplaceNode) Help() string {
	return "<keyspace> <data_center> <node_id> <new_server_ip:new_server_por>"
}

func (c *commandClusterReplaceNode) Do(vastoClient *vs.VastoClient, args []string, commandEnv *commandEnv, writer io.Writer) (err error) {

	if len(args) != 4 {
		return errInvalidArguments
	}
	keyspace := args[0]
	dc := args[1]
	nodeId, err := strconv.ParseUint(args[2], 10, 32)
	if err != nil {
		return errInvalidArguments
	}
	newAddress := args[3]

	return vastoClient.ReplaceNode(keyspace, dc, uint32(nodeId), newAddress)

}
