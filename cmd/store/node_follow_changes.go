package store

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/storage/codec"
	"github.com/chrislusf/vasto/util/on_interrupt"
	"google.golang.org/grpc"
)

const (
	syncProgressFlushInterval = time.Minute
)

func (n *node) followChanges(grpcConnection *grpc.ClientConn) error {

	client := pb.NewVastoStoreClient(grpcConnection)

	nextSegment, nextOffset, err := n.getProgress()
	if err != nil {
		log.Printf("read node %d follow progress: %v", n.id, err)
	}

	request := &pb.PullUpdateRequest{
		NodeId:  uint32(n.id),
		Segment: nextSegment,
		Offset:  nextOffset,
		Limit:   1000,
	}

	stream, err := client.TailBinlog(context.Background(), request)
	if err != nil {
		return fmt.Errorf("client.TailBinlog: %v", err)
	}

	flushCounter := 0
	flushTime := time.Now()

	on_interrupt.OnInterrupt(func() {
		if flushCounter > 0 {
			err = n.setProgress(nextSegment, nextOffset)
			if err != nil {
				log.Printf("set node %d follow progress: %v", n.id, err)
			}
		}
	}, nil)

	for {

		// println("TailBinlog receive from", n.id)

		changes, err := stream.Recv()
		if err != nil {
			return fmt.Errorf("pull changes: %v", err)
		}

		for _, entry := range changes.Entries {

			// fmt.Printf("received entry: %v", entry.String())

			flushCounter++

			b, err := n.db.Get(entry.Key)

			// process deletes
			if entry.IsDelete {
				if err == nil && len(b) > 0 {
					row := codec.FromBytes(b)
					if row.IsExpired() {
						continue
					}
					if row.UpdatedAtNs > entry.UpdatedAtNs {
						continue
					}
					n.db.Delete(entry.Key)
				}
				continue
			}

			// process put
			t := &codec.Entry{
				PartitionHash: entry.PartitionHash,
				UpdatedAtNs:   entry.UpdatedAtNs,
				TtlSecond:     entry.TtlSecond,
				Value:         entry.Value,
			}
			if err != nil || len(b) == 0 {
				n.db.Put(entry.Key, t.ToBytes())
				continue
			}
			row := codec.FromBytes(b)
			if row.IsExpired() {
				if !t.IsExpired() {
					n.db.Put(entry.Key, t.ToBytes())
					continue
				}
			} else {
				if row.UpdatedAtNs > entry.UpdatedAtNs {
					continue
				}
				n.db.Put(entry.Key, t.ToBytes())
				continue
			}

		}

		if flushCounter >= 1000 || flushTime.Add(syncProgressFlushInterval).Before(time.Now()) {

			err = n.setProgress(changes.NextSegment, changes.NextOffset)
			if err != nil {
				log.Printf("set node %d follow progress: %v", n.id, err)
			}

			flushCounter = 0
			flushTime = time.Now()
		}

		// set the nextSegment and nextOffset for OnInterrupt()
		nextSegment, nextOffset = changes.NextSegment, changes.NextOffset

	}

	return nil
}
