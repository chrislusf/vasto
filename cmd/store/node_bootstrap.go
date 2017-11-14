package store

import (
	"fmt"

	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/util"
	"github.com/tecbot/gorocksdb"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"io"
	"log"
	"time"
)

/*
bootstrap ensure current node is bootstrapped and can be synced by binlog tailing.
1. checks whether the binlog offset is behind any other nodes, if not, return
2. delete all local data and binlog offset
3. starts to add changes to sstable
4. get the new offsets
*/
func (n *node) bootstrap() {

	log.Printf("starts following node %d ...", n.id)

	util.RetryForever(func() error {
		return n.doFollow()
	}, 2*time.Second)

}

func (n *node) doBootstrapCopy() error {
	node, _, ok := n.clusterListener.GetNode(n.id)

	if !ok {
		return fmt.Errorf("node %d not found", n.id)
	}

	if node == nil {
		return fmt.Errorf("node %d is missing", n.id)
	}

	log.Printf("connecting to node %d at %s", n.id, node.GetAdminAddress())

	grpcConnection, err := grpc.Dial(node.GetAdminAddress(), grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("fail to dial: %v", err)
	}
	defer grpcConnection.Close()

	log.Printf("connected  to node %d at %s", n.id, node.GetAdminAddress())

	segment, offset, err := n.writeToSst(grpcConnection)

	if err != nil {
		return err
	}

	return n.setProgress(segment, offset)

}

func (n *node) writeToSst(grpcConnection *grpc.ClientConn) (segment uint32, offset uint64, err error) {

	client := pb.NewVastoStoreClient(grpcConnection)

	request := &pb.BootstrapCopyRequest{
		NodeId: uint32(n.id),
	}

	stream, err := client.BootstrapCopy(context.Background(), request)
	if err != nil {
		return 0, 0, fmt.Errorf("client.TailBinlog: %v", err)
	}

	err = n.db.AddSstByWriter(func(w *gorocksdb.SSTFileWriter) error {

		for {

			// println("TailBinlog receive from", n.id)

			response, err := stream.Recv()
			if err == io.EOF {
				return err
			}
			if err != nil {
				return fmt.Errorf("bootstrap copy: %v", err)
			}

			for _, keyValue := range response.KeyValues {

				err = w.Add(keyValue.Key, keyValue.Value)
				if err != nil {
					return err
				}

			}

			if response.BinlogTailProgress != nil {
				segment = response.BinlogTailProgress.Segment
				offset = response.BinlogTailProgress.Offset
			}

		}

	})

	return
}
