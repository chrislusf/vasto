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

	r := c.client.ClusterListener.GetClusterRing(*c.client.Option.Keyspace)

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
				NodeId: uint32(n.GetId()),
			}

			defer close(ch)

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

					// fmt.Fprintf(writer, "%v,%v\n", string(keyValue.Key), string(keyValue.Value))

					ch <- keyValue

				}

			}

		})

	}

	pq := make(priorityQueue, r.ExpectedSize())

	for i := 0; i < r.ExpectedSize(); i++ {
		if chans[i] == nil {
			continue
		}
		keyValue := <-chans[i]
		pq[i] = &item{
			KeyValue:  keyValue,
			chanIndex: i,
		}
	}
	heap.Init(&pq)

	for pq.Len() > 0 {
		t := heap.Pop(&pq).(*item)
		fmt.Fprintf(writer, "%v,%v\n", string(t.Key), string(t.Value))
		newT, hasMore := <-chans[t.chanIndex]
		if hasMore {
			heap.Push(&pq, &item{
				KeyValue:  newT,
				chanIndex: t.chanIndex,
			})
			heap.Fix(&pq, len(pq)-1)
		}
	}

	return nil

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
	*pq = old[0 : n-1]
	return item
}
