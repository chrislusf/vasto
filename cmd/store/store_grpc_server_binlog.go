package store

import (
	"fmt"
	"github.com/chrislusf/vasto/pb"
	"github.com/dgryski/go-jump"
	"golang.org/x/net/context"
	"io"
	"log"
	"time"
)

// TailBinlog sends all data if PullUpdateRequest's TargetClusterSize==0,
// or sends all data belong to TargetShardId in cluster of TargetClusterSize
func (ss *storeServer) TailBinlog(request *pb.PullUpdateRequest, stream pb.VastoStore_TailBinlogServer) error {

	log.Printf("TailBinlog %v", request)

	shard, found := ss.keyspaceShards.getShard(request.Keyspace, shard_id(request.ShardId))
	if !found || shard.isShutdown {
		return fmt.Errorf("shard: %s.%d not found", request.Keyspace, request.ShardId)
	}
	segment := uint32(request.Segment)
	offset := int64(request.Offset)
	limit := int(request.Limit)

	// println("TailBinlog server, segment", segment, "offset", offset, "limit", limit)

	if !shard.lm.HasSegment(segment) {

		t := &pb.PullUpdateResponse{
			OutOfSync: true,
		}

		if err := stream.Send(t); err != nil {
			return err
		}

		start, stop := shard.lm.GetSegmentRange()

		return fmt.Errorf("out of sync client reads segment %d offset %d, only has segment [%d,%d]",
			segment, offset, start, stop)

	}

	targetShardId := int32(request.TargetShardId)
	targetClusterSize := int(request.TargetClusterSize)
	if targetClusterSize > 0 && targetShardId != int32(request.ShardId) {
		limit *= targetClusterSize
	}

	defer func() {
		log.Printf("TailBinlog completed shard %v for %v", shard.String(), request.Origin)
	}()

	for {

		// println("TailBinlog server reading entries, segment", segment, "offset", offset, "limit", limit)
		// log.Printf("TailBinlog shard %v %v read entries %d:%d", shard.String(), request.Origin, segment, offset)

		entries, nextOffset, err := shard.lm.ReadEntries(segment, offset, limit)
		if err == io.EOF {
			segment += 1
		} else if err != nil {
			return fmt.Errorf("failed to read segment %d offset %d: %v", segment, offset, err)
		} else if len(entries) <= 100 {
			time.Sleep(100 * time.Millisecond)
			entries, nextOffset, err = shard.lm.ReadEntries(segment, offset, limit)
			if err == io.EOF {
				segment += 1
			} else if err != nil {
				return fmt.Errorf("failed to read segment %d offset %d: %v", segment, offset, err)
			}
		}

		// log.Printf("shard %v read for %v: %d, @ %d:%d, next %d", shard.String(), request.Origin, len(entries), segment, offset, nextOffset)

		t := &pb.PullUpdateResponse{
			NextSegment: segment,
			NextOffset:  uint64(nextOffset),
		}

		for _, entry := range entries {

			// log.Printf("shard %v send0 %v: %v offset:%d", shard.String(), request.Origin, string(entry.Key), offset)
			if targetClusterSize > 0 && jump.Hash(entry.GetPartitionHash(), targetClusterSize) != targetShardId {
				// log.Printf("shard %v send %v skipped: %v, hash:%v, targetClusterSize:%d, targetShardId:%d ", shard.String(), request.Origin, string(entry.Key), entry.PartitionHash, targetClusterSize, targetShardId)
				continue
			}

			// log.Printf("shard %v send %v: %v", shard.String(), request.Origin, string(entry.Key))

			t.Entries = append(t.Entries, entry)
		}

		if err := stream.Send(t); err != nil {
			log.Printf("TailBinlog shard %v send %v: %v", shard.String(), request.Origin, err)
			return err
		}

		offset = nextOffset

	}

	return nil
}

func (ss *storeServer) CheckBinlog(ctx context.Context, request *pb.CheckBinlogRequest) (*pb.CheckBinlogResponse, error) {

	node, found := ss.keyspaceShards.getShard(request.Keyspace, shard_id(request.ShardId))
	if !found {
		return nil, fmt.Errorf("checkbinlog: %s shard %d not found", request.Keyspace, request.ShardId)
	}

	earliestSegment, latestSegment := node.lm.GetSegmentRange()

	return &pb.CheckBinlogResponse{
		ShardId:         request.ShardId,
		EarliestSegment: earliestSegment,
		LatestSegment:   latestSegment,
	}, nil

}
