package store

import (
	"context"
	"fmt"

	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/storage/codec"
	"google.golang.org/grpc"
	"log"
)

func (n *node) followChanges(grpcConnection *grpc.ClientConn) error {

	client := pb.NewVastoStoreClient(grpcConnection)

	nextSegment, nextOffset, err := n.getProgress()
	if err != nil {
		log.Printf("read node %d follow progress: %v", n.id, err)
	}

	request := &pb.PullUpdateRequest{
		Segment: nextSegment,
		Offset:  nextOffset,
		Limit:   1000,
	}

	stream, err := client.PullChanges(context.Background(), request)
	if err != nil {
		return fmt.Errorf("client.PullChanges: %v", err)
	}

	flushCounter := 0

	for {
		changes, err := stream.Recv()
		if err != nil {
			return fmt.Errorf("pull changes: %v", err)
		}

		for _, entry := range changes.Entries {

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

		if flushCounter >= 1000 {

			err = n.setProgress(changes.NextSegment, changes.NextOffset)
			if err != nil {
				log.Printf("set node %d follow progress: %v", n.id, err)
			}

			flushCounter = 0
		}

	}

	return nil
}
