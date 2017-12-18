package store

import (
	"github.com/chrislusf/vasto/pb"
	"fmt"
	"log"
	"github.com/chrislusf/vasto/storage/codec"
	"github.com/dgryski/go-jump"
)

// BootstrapCopy sends all data if BootstrapCopyRequest's TargetClusterSize==0,
// or sends all data belong to TargetShardId in cluster of TargetClusterSize
func (ss *storeServer) BootstrapCopy(request *pb.BootstrapCopyRequest, stream pb.VastoStore_BootstrapCopyServer) error {

	log.Printf("BootstrapCopy %v", request)

	node, found := ss.findDbReplica(request.Keyspace, request.ShardId)
	if !found {
		return fmt.Errorf("shard: %s.%d not found", request.Keyspace, request.ShardId)
	}

	segment, offset := node.lm.GetSegmentOffset()

	// println("server", shard.serverId, "shard", shard.id, "segment", segment, "offset", offset)

	targetShardId := int32(request.TargetShardId)
	targetClusterSize := int(request.TargetClusterSize)
	batchSize := 1024
	if targetClusterSize > 0 && targetShardId != int32(request.ShardId) {
		batchSize *= targetClusterSize
	}

	err := node.db.FullScan(batchSize, func(rows []*pb.KeyValue) error {

		var filteredRows []*pb.KeyValue
		if targetClusterSize > 0 {
			for _, row := range rows {
				partitionHash := codec.GetPartitionHashFromBytes(row.Value)
				if jump.Hash(partitionHash, targetClusterSize) == targetShardId {
					t := row
					filteredRows = append(filteredRows, t)
				}
			}
		} else {
			filteredRows = rows
		}

		t := &pb.BootstrapCopyResponse{
			KeyValues: filteredRows,
		}
		if err := stream.Send(t); err != nil {
			return err
		}
		return nil
	})

	t := &pb.BootstrapCopyResponse{
		BinlogTailProgress: &pb.BootstrapCopyResponse_BinlogTailProgress{
			Segment: segment,
			Offset:  uint64(offset),
		},
	}
	if err := stream.Send(t); err != nil {
		return err
	}

	return err
}

func (ss *storeServer) findDbReplica(keyspace string, shardId uint32) (replica *shard, found bool) {

	shards, found := ss.keyspaceShards.getShards(keyspace)
	if !found {
		return nil, false
	}

	for _, shard := range shards {
		if int(shard.id) == int(shardId) {
			replica = shard
			return replica, true
		}
	}
	return nil, false
}
