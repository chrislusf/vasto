package shell

import (
	"fmt"
	"io"

	"context"
	"github.com/chrislusf/vasto/client"
	"github.com/chrislusf/vasto/pb"
	"google.golang.org/grpc"
	"log"
	"sync/atomic"
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
	return "keys|key_value"
}

func (c *CommandDump) SetCilent(client *client.VastoClient) {
	c.client = client
}

func (c *CommandDump) Do(args []string, env map[string]string, writer io.Writer) (doError error) {

	isKeysOnly := true
	if len(args) > 0 && args[0] == "key_value" {
		isKeysOnly = false
	}

	cluster, found := c.client.ClusterListener.GetCluster(*c.client.Option.Keyspace)
	if !found {
		return fmt.Errorf("no keyspace %s", *c.client.Option.Keyspace)
	}

	chans := make([]chan *pb.KeyValue, cluster.ExpectedSize())

	var counter int64

	for i := 0; i < cluster.ExpectedSize(); i++ {

		_, _, ok := cluster.GetNode(i)
		if !ok {
			continue
		}

		ch := make(chan *pb.KeyValue)
		chans[i] = ch

		go cluster.WithConnection("dump", i, func(node *pb.ClusterNode, grpcConnection *grpc.ClientConn) error {

			client := pb.NewVastoStoreClient(grpcConnection)

			request := &pb.BootstrapCopyRequest{
				Keyspace:          *c.client.Option.Keyspace,
				ShardId:           uint32(node.ShardInfo.ShardId),
				ClusterSize:       uint32(cluster.ExpectedSize()),
				TargetClusterSize: uint32(cluster.ExpectedSize()),
				TargetShardId:     uint32(node.ShardInfo.ShardId),
				Origin:            "shell dump",
			}

			defer close(ch)

			stream, err := client.BootstrapCopy(context.Background(), request)
			if err != nil {
				return fmt.Errorf("client.dump: %v", err)
			}

			for {
				response, err := stream.Recv()
				if err == io.EOF {
					return nil
				}
				if err != nil {
					doError = fmt.Errorf("dump: %v", err)
					log.Printf("dump: %v", err)
					return err
				}

				for _, keyValue := range response.KeyValues {

					// fmt.Fprintf(writer, "%v,%v\n", string(keyValue.Key), string(keyValue.Value))

					ch <- keyValue
					atomic.AddInt64(&counter, 1)

				}
			}
			return nil
		})

	}

	err := pb.MergeSorted(chans, func(t *pb.KeyValue) error {
		if isKeysOnly {
			fmt.Fprintf(writer, "%v\n", string(t.Key))
		} else {
			fmt.Fprintf(writer, "%v,%v\n", string(t.Key), string(t.Value))
		}
		return nil
	})

	if err != nil {
		return fmt.Errorf("merge sorted: %v", err)
	}

	fmt.Fprintf(writer, "\n(%v rows)\n", counter)

	return

}
