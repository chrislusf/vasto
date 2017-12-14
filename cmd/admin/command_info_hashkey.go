package admin

import (
	"io"
	"github.com/chrislusf/vasto/pb"
	"strconv"
	"github.com/dgryski/go-jump"
	"github.com/chrislusf/vasto/util"
	"fmt"
)

func init() {
	commands = append(commands, &CommandInfo{})
}

type CommandInfo struct {
	masterClient pb.VastoMasterClient
}

func (c *CommandInfo) Name() string {
	return "info"
}

func (c *CommandInfo) Help() string {
	return "hashkey <key> <cluster_size>  // see the key will go to which bucket"
}

func (c *CommandInfo) SetMasterCilent(masterClient pb.VastoMasterClient) {
	c.masterClient = masterClient
}

func (c *CommandInfo) Do(args []string, out io.Writer) (err error) {

	if args[0] == "hashkey" {
		if len(args) != 3 {
			return InvalidArguments
		}
		clusterSize, err := strconv.ParseUint(args[2], 10, 64)
		if err != nil {
			return err
		}

		t := jump.Hash(util.Hash([]byte(args[1])), int(clusterSize))
		fmt.Fprintf(out, "bucket: %d\n", t)

	}

	return nil
}
