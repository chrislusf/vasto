package shell

import (
	"fmt"
	"io"

	"context"
	"github.com/chrislusf/vasto/cmd/client"
	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/topology"
	"google.golang.org/grpc"
)

func init() {
	commands = append(commands, &CommandDump{})
}

type CommandDump struct {
	client *client.VastoClient
}

func (c *CommandDump) Name() string {
	return "dump"
}

func (c *CommandDump) Help() string {
	return "key"
}

func (c *CommandDump) SetCilent(client *client.VastoClient) {
	c.client = client
}

func (c *CommandDump) Do(args []string, env map[string]string, writer io.Writer) error {

	for i := 0; i < c.client.ClusterListener.ExpectedSize(); i++ {

		n, _, ok := c.client.ClusterListener.GetNode(i)
		if !ok {
			continue
		}

		c.client.ClusterListener.WithConnection(i, func(node topology.Node, grpcConnection *grpc.ClientConn) error {
			client := pb.NewVastoStoreClient(grpcConnection)

			request := &pb.BootstrapCopyRequest{
				NodeId: uint32(n.GetId()),
			}

			stream, err := client.BootstrapCopy(context.Background(), request)
			if err != nil {
				return fmt.Errorf("client.dump: %v", err)
			}

			for {

				// println("TailBinlog receive from", n.id)

				response, err := stream.Recv()
				if err == io.EOF {
					return nil
				}
				if err != nil {
					return fmt.Errorf("dump: %v", err)
				}

				for _, keyValue := range response.KeyValues {

					fmt.Fprintf(writer, "%v,%v\n", string(keyValue.Key), string(keyValue.Value))

				}

			}

		})

	}

	return nil

}
