package admin

import (
	"fmt"
	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/util"
	"github.com/dgryski/go-jump"
	"io"
	"strconv"
)

func init() {
	commands = append(commands, &commandInfo{})
}

type commandInfo struct {
	masterClient pb.VastoMasterClient
}

func (c *commandInfo) Name() string {
	return "info"
}

func (c *commandInfo) Help() string {
	return "hashkey <key> <cluster_size>  // see the key will go to which bucket"
}

func (c *commandInfo) SetMasterCilent(masterClient pb.VastoMasterClient) {
	c.masterClient = masterClient
}

func (c *commandInfo) Do(args []string, out io.Writer) (err error) {

	if args[0] == "hashkey" {
		if len(args) != 3 {
			return invalidArguments
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
