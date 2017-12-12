package shell

import (
	"fmt"
	"io"

	"bytes"
	"container/heap"
	"context"
	"github.com/chrislusf/vasto/cmd/client"
	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/topology"
	"google.golang.org/grpc"
	"log"
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

	r, found := c.client.ClusterListener.GetClusterRing(*c.client.Option.Keyspace)
	if !found {
		return fmt.Errorf("no keyspace %s", *c.client.Option.Keyspace)
	}

	chans := make([]chan *pb.KeyValue, r.ExpectedSize())

	for i := 0; i < r.ExpectedSize(); i++ {

		n, _, ok := r.GetNode(i)
		if !ok {
			continue
		}

		ch := make(chan *pb.KeyValue)
		chans[i] = ch

		go r.WithConnection(i, func(node topology.Node, grpcConnection *grpc.ClientConn) error {

			client := pb.NewVastoStoreClient(grpcConnection)

			request := &pb.BootstrapCopyRequest{
				Keyspace: *c.client.Option.Keyspace,
				ShardId:  uint32(n.GetId()),
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

				}
			}
			println("TailBinlog stop 3 for", n.GetId())
			return nil
		})

	}

	pq := make(priorityQueue, 0, r.ExpectedSize())

	for i := 0; i < r.ExpectedSize(); i++ {
		if chans[i] == nil {
			continue
		}
		keyValue := <-chans[i]
		if keyValue != nil {
			pq = append(pq, &item{
				KeyValue:  keyValue,
				chanIndex: i,
			})
		}
	}
	heap.Init(&pq)

	for pq.Len() > 0 {
		t := heap.Pop(&pq).(*item)
		if isKeysOnly {
			fmt.Fprintf(writer, "%v\n", string(t.Key))
		} else {
			fmt.Fprintf(writer, "%v,%v\n", string(t.Key), string(t.Value))
		}
		newT, hasMore := <-chans[t.chanIndex]
		if hasMore {
			heap.Push(&pq, &item{
				KeyValue:  newT,
				chanIndex: t.chanIndex,
			})
			heap.Fix(&pq, len(pq)-1)
		}
	}

	return

}

// An item is something we manage in a priority queue.
type item struct {
	*pb.KeyValue
	chanIndex int
}

// A priorityQueue implements heap.Interface and holds Items.
type priorityQueue []*item

func (pq priorityQueue) Len() int { return len(pq) }

func (pq priorityQueue) Less(i, j int) bool {
	return bytes.Compare(pq[i].Key, pq[j].Key) < 0
}

func (pq priorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *priorityQueue) Push(x interface{}) {
	item := x.(*item)
	*pq = append(*pq, item)
}

func (pq *priorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0: n-1]
	return item
}
