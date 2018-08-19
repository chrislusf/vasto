package shell

import (
	"fmt"
	"io"

	"context"
	"github.com/chrislusf/glog"
	"github.com/chrislusf/vasto/goclient/vs"
	"github.com/chrislusf/vasto/pb"
	"google.golang.org/grpc"
	"strconv"
)

func init() {
	commands = append(commands, &commandDump{})
}

type commandDump struct {
}

func (c *commandDump) Name() string {
	return "dump"
}

func (c *commandDump) Help() string {
	return "keys|key_value [limit]"
}

func (c *commandDump) Do(vastoClient *vs.VastoClient, args []string, commandEnv *commandEnv, writer io.Writer) (doError error) {

	isKeysOnly := true
	if len(args) > 0 && args[0] == "key_value" {
		isKeysOnly = false
	}
	limit := uint64(0)
	if len(args) > 1 {
		limit, doError = strconv.ParseUint(args[1], 10, 64)
		if doError != nil {
			return doError
		}
	}

	if commandEnv.clusterClient == nil {
		return errNoKeyspaceSelected
	}

	cluster, err := commandEnv.clusterClient.GetCluster()
	if err != nil {
		return err
	}

	chans := make([]chan *pb.RawKeyValue, cluster.ExpectedSize())

	for i := 0; i < cluster.ExpectedSize(); i++ {

		_, ok := cluster.GetNode(i, 0)
		if !ok {
			continue
		}

		ch := make(chan *pb.RawKeyValue)
		chans[i] = ch

		go cluster.WithConnection("dump", i, func(node *pb.ClusterNode, grpcConnection *grpc.ClientConn) error {

			storeClient := pb.NewVastoStoreClient(grpcConnection)

			request := &pb.BootstrapCopyRequest{
				Keyspace:          commandEnv.keyspace,
				ShardId:           uint32(node.ShardInfo.ShardId),
				ClusterSize:       uint32(cluster.ExpectedSize()),
				TargetClusterSize: uint32(cluster.ExpectedSize()),
				TargetShardId:     uint32(node.ShardInfo.ShardId),
				Origin:            "shell dump",
				Limit:             limit,
			}

			defer close(ch)

			stream, err := storeClient.BootstrapCopy(context.Background(), request)
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
					glog.Errorf("dump: %v", err)
					return err
				}

				for _, keyValue := range response.KeyValues {

					// fmt.Fprintf(writer, "%v,%v\n", string(keyValue.Key), string(keyValue.Value))

					ch <- keyValue

				}
			}

		})

	}

	counter, err := pb.MergeSorted(chans, int64(limit), func(t *pb.RawKeyValue) error {
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
