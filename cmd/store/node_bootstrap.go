package store

import (
	"fmt"
	"io"
	"log"

	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/topology"
	"github.com/tecbot/gorocksdb"
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
		// log.Printf("bootstrap node %d is not needed", n.id)
		return nil
	}

	log.Printf("bootstrap from server %d ...", bestPeerToCopy)

	return n.clusterRing.WithConnection(bestPeerToCopy, func(node topology.Node, grpcConnection *grpc.ClientConn) error {
		_, canTailBinlog, err := n.checkBinlogAvailable(grpcConnection)
		if err != nil {
			return err
		}
		if !canTailBinlog {
			return n.doBootstrapCopy(grpcConnection)
		}
		return nil
	})

}

func (n *node) checkBinlogAvailable(grpcConnection *grpc.ClientConn) (latestSegment uint32, canTailBinlog bool, err error) {

	segment, _, hasProgress, err := n.getProgress()

	// println("node", n.id, "segment", segment, "hasProgress", hasProgress, "err", err)

	if !hasProgress {
		return 0, false, nil
	}

	if err != nil {
		return 0, false, err
	}

	client := pb.NewVastoStoreClient(grpcConnection)

	resp, err := client.CheckBinlog(n.ctx, &pb.CheckBinlogRequest{
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
		return fmt.Errorf("writeToSst: %v", err)
	}

	return n.setProgress(segment, offset)

}

func (n *node) writeToSst(grpcConnection *grpc.ClientConn) (segment uint32, offset uint64, err error) {

	client := pb.NewVastoStoreClient(grpcConnection)

	request := &pb.BootstrapCopyRequest{
		NodeId: uint32(n.id),
	}

	stream, err := client.BootstrapCopy(n.ctx, request)
	if err != nil {
		return 0, 0, fmt.Errorf("client.TailBinlog: %v", err)
	}

	err = n.db.AddSstByWriter(func(w *gorocksdb.SSTFileWriter) (int, error) {

		var counter int

		for {

			// println("TailBinlog receive from", n.id)

			response, err := stream.Recv()
			if err == io.EOF {
				return counter, nil
			}
			if err != nil {
				return counter, fmt.Errorf("bootstrap copy: %v", err)
			}

			for _, keyValue := range response.KeyValues {

				fmt.Printf("add to sst: %v\n", keyValue.String())

				err = w.Add(keyValue.Key, keyValue.Value)
				if err != nil {
					return counter, fmt.Errorf("add to sst: %v", err)
				}
				counter++

			}

			if response.BinlogTailProgress != nil {
				segment = response.BinlogTailProgress.Segment
				offset = response.BinlogTailProgress.Offset
			}

		}

	})

	return
}
