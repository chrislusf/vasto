package store

import (
	"context"
	"fmt"
	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/storage/codec"
	"github.com/chrislusf/vasto/util"
	"google.golang.org/grpc"
)

// prev should be 1 or 2, previous node or previous previous node.
func (ss *storeServer) syncChanges(prev int) error {

	targetNode := int(*ss.option.Id) - prev
	if targetNode < 0 {
		targetNode += ss.clusterListener.CurrentSize()
	}

	node := ss.clusterListener.GetNode(targetNode)

	grpcConnection, err := grpc.Dial(node.GetAddress(), grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("fail to dial: %v", err)
	}
	defer grpcConnection.Close()

	client := pb.NewVastoStoreClient(grpcConnection)

	nextSegmentKey := []byte(fmt.Sprintf("%d.next.segment", targetNode))
	nextOffsetKey := []byte(fmt.Sprintf("%d.next.offset", targetNode))
	nextSegment := uint32(0)
	nextOffset := uint64(0)
	t, err := ss.db.Get(nextSegmentKey)
	if err == nil && len(t) > 0 {
		nextSegment = util.BytesToUint32(t)
	}
	t, err = ss.db.Get(nextOffsetKey)
	if err == nil && len(t) > 0 {
		nextOffset = util.BytesToUint64(t)
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

			b, err := ss.db.Get(entry.Key)

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
					ss.db.Delete(entry.Key)
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
				ss.db.Put(entry.Key, t.ToBytes())
				continue
			}
			row := codec.FromBytes(b)
			if row.IsExpired() {
				if !t.IsExpired() {
					ss.db.Put(entry.Key, t.ToBytes())
					continue
				}
			} else {
				if row.UpdatedAtNs > entry.UpdatedAtNs {
					continue
				}
				ss.db.Put(entry.Key, t.ToBytes())
				continue
			}

		}

		if flushCounter >= 1000 {

			ss.db.Put(nextSegmentKey, util.Uint32toBytes(changes.NextSegment))
			ss.db.Put(nextOffsetKey, util.Uint64toBytes(changes.NextOffset))

			flushCounter = 0
		}

	}

	return nil
}
