package store

import (
	"fmt"
	"io"
	"log"

	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/topology"
	"github.com/tecbot/gorocksdb"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

/*
bootstrap ensure current node is bootstrapped and can be synced by binlog tailing.
1. checks whether the binlog offset is behind any other nodes, if not, return
2. delete all local data and binlog offset
3. starts to add changes to sstable
4. get the new offsets
*/
func (n *node) bootstrap() error {

	bestPeerToCopy, isNeeded := n.isBootstrapNeeded()
	if !isNeeded {
		return nil
	}

	log.Printf("bootstrap from server %d ...", bestPeerToCopy)

	return n.withConnection(bestPeerToCopy, func(node topology.Node, grpcConnection *grpc.ClientConn) error {
		_, ok, err := n.checkBinlogAvailable(grpcConnection)
		if err != nil {
			return err
		}
		if !ok {
			return n.doBootstrapCopy(grpcConnection)
		}
		return nil
	})

}

func (n *node) checkBinlogAvailable(grpcConnection *grpc.ClientConn) (latestSegment uint32, isAvailable bool, err error) {

	segment, _, err := n.getProgress()

	if err != nil {
		return 0, false, err
	}

	client := pb.NewVastoStoreClient(grpcConnection)

	resp, err := client.CheckBinlog(context.Background(), &pb.CheckBinlogRequest{
		NodeId: uint32(n.id),
	})
	if err != nil {
		return 0, false, err
	}

	return resp.LatestSegment, resp.EarliestSegment <= segment, nil

}

func (n *node) doBootstrapCopy(grpcConnection *grpc.ClientConn) error {

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
