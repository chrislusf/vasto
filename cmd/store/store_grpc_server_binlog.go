package store

import (
	"fmt"
	"io"
	"log"
	"github.com/chrislusf/vasto/pb"
	"golang.org/x/net/context"
	"time"
	"github.com/dgryski/go-jump"
)

// TailBinlog sends all data if PullUpdateRequest's TargetClusterSize==0,
// or sends all data belong to TargetShardId in cluster of TargetClusterSize
func (ss *storeServer) TailBinlog(request *pb.PullUpdateRequest, stream pb.VastoStore_TailBinlogServer) error {

	log.Printf("TailBinlog %v", request)

	node, found := ss.findDbReplica(request.Keyspace, request.ShardId)
	if !found || node.isShutdown {
		return fmt.Errorf("shard: %s.%d not found", request.Keyspace, request.ShardId)
	}
	segment := uint32(request.Segment)
	offset := int64(request.Offset)
	limit := int(request.Limit)

	// println("TailBinlog server, segment", segment, "offset", offset, "limit", limit)

	if !node.lm.HasSegment(segment) {

		t := &pb.PullUpdateResponse{
			OutOfSync: true,
		}

		if err := stream.Send(t); err != nil {
			return err
		}

		start, stop := node.lm.GetSegmentRange()

		return fmt.Errorf("out of sync client reads segment %d offset %d, only has segment [%d,%d]",
			segment, offset, start, stop)

	}

	targetShardId := int32(request.TargetShardId)
	targetClusterSize := int(request.TargetClusterSize)
	if targetClusterSize > 0 && targetShardId != int32(request.ShardId) {
		limit *= targetClusterSize
	}

	for {

		// println("TailBinlog server reading entries, segment", segment, "offset", offset, "limit", limit)

		entries, nextOffset, err := node.lm.ReadEntries(segment, offset, limit)
		if err == io.EOF {
			segment += 1
		} else if err != nil {
			return fmt.Errorf("failed to read segment %d offset %d: %v", segment, offset, err)
		} else if len(entries) <= 100 {
			time.Sleep(100 * time.Millisecond)
			entries, nextOffset, err = node.lm.ReadEntries(segment, offset, limit)
			if err == io.EOF {
				segment += 1
			} else if err != nil {
				return fmt.Errorf("failed to read segment %d offset %d: %v", segment, offset, err)
			}
		}
		// println("len(entries) =", len(entries), "offset", offset, "next offset", nextOffset)

		offset = nextOffset

		t := &pb.PullUpdateResponse{
			NextSegment: segment,
			NextOffset:  uint64(nextOffset),
		}

		for _, entry := range entries {
			if !entry.IsValid() {
				log.Printf("read an invalid entry: %+v", entry)
				continue
			}
			if targetClusterSize > 0 && jump.Hash(entry.PartitionHash, targetClusterSize) != targetShardId {
				continue
			}
			t.Entries = append(t.Entries, &pb.UpdateEntry{
				PartitionHash: entry.PartitionHash,
				UpdatedAtNs:   entry.UpdatedNanoSeconds,
				TtlSecond:     entry.TtlSecond,
				IsDelete:      entry.IsDelete,
				Key:           entry.Key,
				Value:         entry.Value,
			})
		}

		if err := stream.Send(t); err != nil {
			return err
		}

	}

	return nil
}

func (ss *storeServer) CheckBinlog(ctx context.Context, request *pb.CheckBinlogRequest) (*pb.CheckBinlogResponse, error) {

	node, found := ss.findDbReplica(request.Keyspace, request.NodeId)
	if !found {
		return nil, fmt.Errorf("shard: %s.%d not found", request.Keyspace, request.NodeId)
	}

	earliestSegment, latestSegment := node.lm.GetSegmentRange()

	return &pb.CheckBinlogResponse{
		NodeId:          request.NodeId,
		EarliestSegment: earliestSegment,
		LatestSegment:   latestSegment,
	}, nil

}
