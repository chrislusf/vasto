package shell

import (
	"io"
	"strconv"

	"github.com/chrislusf/vasto/goclient/vs"
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
	return "<keyspace> <node_id> <new_server_ip:new_server_por>"
}

func (c *commandClusterReplaceNode) Do(vastoClient *vs.VastoClient, args []string, commandEnv *commandEnv, writer io.Writer) (err error) {

	if len(args) != 3 {
		return errInvalidArguments
	}
	keyspace := args[0]
	nodeId, err := strconv.ParseUint(args[1], 10, 32)
	if err != nil {
		return errInvalidArguments
	}
	newAddress := args[2]

	return vastoClient.ReplaceNode(keyspace, uint32(nodeId), newAddress)

}
