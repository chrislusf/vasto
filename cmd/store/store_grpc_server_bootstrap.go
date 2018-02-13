package store

import (
	"bytes"
	"fmt"
	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/storage/codec"
	"github.com/dgryski/go-jump"
	"github.com/golang/glog"
)

const (
	BOOTSTRAP_COPY_BATCH_SIZE = 1024
)

// BootstrapCopy sends all data if BootstrapCopyRequest's TargetClusterSize==0,
// or sends all data belong to TargetShardId in cluster of TargetClusterSize
func (ss *storeServer) BootstrapCopy(request *pb.BootstrapCopyRequest, stream pb.VastoStore_BootstrapCopyServer) error {

	glog.V(1).Infof("BootstrapCopy %v", request)

	shard, found := ss.keyspaceShards.getShard(request.Keyspace, shard_id(request.ShardId))
	if !found {
		return fmt.Errorf("BootstrapCopy: %s shard %d not found", request.Keyspace, request.ShardId)
	}

	segment, offset := shard.lm.GetSegmentOffset()

	// println("server", shard.serverId, "shard", shard.id, "segment", segment, "offset", offset)

	targetShardId := int32(request.TargetShardId)
	targetClusterSize := int(request.TargetClusterSize)
	currentClusterSize := int(request.ClusterSize)
	currentShardId := int32(shard.id)
	batchSize := BOOTSTRAP_COPY_BATCH_SIZE
	if targetClusterSize > 0 && targetShardId != int32(request.ShardId) {
		batchSize *= targetClusterSize
	}

	err := shard.db.FullScan(batchSize, func(rows []*pb.RawKeyValue) error {

		var filteredRows []*pb.RawKeyValue
		for _, row := range rows {
			if bytes.HasPrefix(row.Key, INTERNAL_PREFIX) {
				continue
			}
			partitionHash := codec.GetPartitionHashFromBytes(row.Value)
			if jump.Hash(partitionHash, currentClusterSize) != currentShardId {
				// glog.V(2).Infof("skipping key=%s currentClusterSize=%d currentShardId=%d", string(row.Key), currentClusterSize, currentShardId)
				continue
			}
			if targetClusterSize > 0 {
				if jump.Hash(partitionHash, targetClusterSize) == targetShardId {
					filteredRows = append(filteredRows, row)
				}
			} else {
				filteredRows = append(filteredRows, row)
			}
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

	glog.V(1).Infof("BootstrapCopy %v completed at %d:%d", request, segment, offset)

	return err
}
