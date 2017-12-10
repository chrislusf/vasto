package store

import (
	"fmt"
	"log"
	"time"

	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/storage/codec"
	"github.com/chrislusf/vasto/topology"
	"google.golang.org/grpc"
)

const (
	syncProgressFlushInterval = time.Minute
)

// implementing PeriodicTask
func (n *node) Keyspace() string {
	return n.keyspace
}

// implementing PeriodicTask
func (n *node) EverySecond() {
	// log.Printf("%s every second", n)
	if n.prevSegment != n.nextSegment || n.prevOffset != n.nextOffset {
		n.setProgress(n.nextSegment, n.nextOffset)
		n.prevSegment, n.prevOffset = n.nextSegment, n.nextOffset
	}
}

func (n *node) followChanges(node topology.Node, grpcConnection *grpc.ClientConn) (err error) {

	client := pb.NewVastoStoreClient(grpcConnection)

	n.nextSegment, n.nextOffset, _, err = n.getProgress()
	if err != nil {
		log.Printf("read node %d follow progress: %v", n.id, err)
	}

	log.Printf("Starting to follow from segment %d offset %d", n.nextSegment, n.nextOffset)

	request := &pb.PullUpdateRequest{
		NodeId:  uint32(n.id),
		Segment: n.nextSegment,
		Offset:  n.nextOffset,
		Limit:   8096,
	}

	stream, err := client.TailBinlog(n.ctx, request)
	if err != nil {
		return fmt.Errorf("client.TailBinlog to server %d %s: %v", node.GetId(), node.GetAdminAddress(), err)
	}

	for {

		// println("TailBinlog receive from", n.id)

		changes, err := stream.Recv()
		if err != nil {
			return fmt.Errorf("pull changes: %v", err)
		}

		log.Printf("%s follow 0 entry: %d", n, len(changes.Entries))

		for _, entry := range changes.Entries {

			// log.Printf("%s follow 1 entry: %v", n, string(entry.Key))

			b, err := n.db.Get(entry.Key)
			if err != nil {
				continue
			}

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

			// log.Printf("%s follow 2 entry: %v, err: %v, len(b)=%d", n, string(entry.Key), err, len(b))

			// process put
			t := &codec.Entry{
				PartitionHash: entry.PartitionHash,
				UpdatedAtNs:   entry.UpdatedAtNs,
				TtlSecond:     entry.TtlSecond,
				Value:         entry.Value,
			}
			if len(b) == 0 {
				// no existing data found
				n.db.Put(entry.Key, t.ToBytes())
				continue
			}

			row := codec.FromBytes(b)
			// log.Printf("%s follow 3 entry: %v, expired %v, %v", n, string(entry.Key), row.IsExpired(), t.IsExpired())
			if row.IsExpired() {
				if !t.IsExpired() {
					log.Printf("%s follow 3 entry: %v", n, string(entry.Key))
					n.db.Put(entry.Key, t.ToBytes())
					continue
				}
			} else {
				// log.Printf("data time %d, existing time %d, delta %d", row.UpdatedAtNs, entry.UpdatedAtNs, row.UpdatedAtNs-entry.UpdatedAtNs)
				if row.UpdatedAtNs > entry.UpdatedAtNs {
					continue
				}
				n.db.Put(entry.Key, t.ToBytes())
				continue
			}
			// log.Printf("%s follow 4 entry: %v", n, string(entry.Key))

		}

		// set the nextSegment and nextOffset for OnInterrupt()
		n.nextSegment, n.nextOffset = changes.NextSegment, changes.NextOffset

	}

	return nil
}
