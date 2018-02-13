package store

import (
	"fmt"
	"time"

	"context"
	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/storage/codec"
	"google.golang.org/grpc"
	"github.com/chrislusf/glog"
)

const (
	syncProgressFlushInterval = time.Minute
)

func (s *shard) followChanges(ctx context.Context, node *pb.ClusterNode, grpcConnection *grpc.ClientConn, sourceShardId int, targetClusterSize int, saveFollowProgress bool) error {

	client := pb.NewVastoStoreClient(grpcConnection)

	nextSegment, nextOffset, _, err := s.loadProgress(node.StoreResource.GetAdminAddress(), shard_id(sourceShardId))
	if err != nil {
		glog.Errorf("read shard %d follow progress: %v", s.id, err)
	}
	glog.V(1).Infof("shard %v follows %d.%d from segment:offset %d:%d", s.String(), node.ShardInfo.ServerId, sourceShardId, nextSegment, nextOffset)

	// set in memory progress
	if saveFollowProgress {
		s.insertInMemoryFollowProgress(node.StoreResource.GetAdminAddress(), shard_id(sourceShardId), nextSegment, nextOffset)
	}

	request := &pb.PullUpdateRequest{
		Keyspace:          s.keyspace,
		ShardId:           uint32(sourceShardId),
		Segment:           nextSegment,
		Offset:            nextOffset,
		Limit:             8096,
		TargetClusterSize: uint32(targetClusterSize),
		TargetShardId:     uint32(s.id),
		Origin:            s.String(),
	}

	stream, err := client.TailBinlog(ctx, request)
	if err != nil {
		return fmt.Errorf("client.TailBinlog to server %d %s: %v", node.ShardInfo.ServerId, node.StoreResource.GetAdminAddress(), err)
	}

	for {

		// println("TailBinlog receive from", s.id)

		changes, err := stream.Recv()
		if err != nil {
			return fmt.Errorf("pull changes: %v", err)
		}

		// glog.V(2).Infof("%s follow 0 entry: %d", s, len(changes.Entries))

		for _, entry := range changes.Entries {

			// process merges
			if entry.GetMerge() != nil {
				merge := entry.GetMerge()
				key := merge.Key
				t := codec.NewMergeEntry(merge, entry.UpdatedAtNs)

				s.db.Merge(key, t.ToBytes())
				continue
			}

			// check local entry
			b, err := s.db.Get(entry.GetKey())
			if err != nil {
				glog.Errorf("%s get %v: %v", s, string(entry.GetKey()), err)
				continue
			}

			// process deletes
			if entry.GetDelete() != nil {
				if err == nil && len(b) > 0 {
					row := codec.FromBytes(b)
					if row.IsExpired() {
						continue
					}
					if row.UpdatedAtNs > entry.UpdatedAtNs {
						continue
					}
					s.db.Delete(entry.GetKey())
				}
				continue
			}

			// process puts
			if entry.GetPut() != nil {
				put := entry.GetPut()
				key := put.Key
				t := codec.NewPutEntry(put, entry.UpdatedAtNs)

				if len(b) == 0 {
					// no existing data found
					s.db.Put(key, t.ToBytes())
					continue
				}
				row := codec.FromBytes(b)
				if row.IsExpired() {
					if !t.IsExpired() {
						glog.V(3).Infof("%s follow 3 entry: %v", s, string(key))
						s.db.Put(key, t.ToBytes())
						continue
					}
				} else {
					if row.UpdatedAtNs > entry.UpdatedAtNs {
						continue
					}
					s.db.Put(key, t.ToBytes())
					continue
				}
				// glog.V(2).Infof("%s follow 4 entry: %v", s, string(entry.Key))
			}

		}

		// set the nextSegment and nextOffset
		nextSegment, nextOffset = changes.NextSegment, changes.NextOffset
		if saveFollowProgress {
			s.updateInMemoryFollowProgressIfPresent(node.StoreResource.GetAdminAddress(), shard_id(sourceShardId), nextSegment, nextOffset)
		}

	}

	if saveFollowProgress {
		s.saveProgress(node.StoreResource.GetAdminAddress(), shard_id(sourceShardId), nextSegment, nextOffset)
	}

	return nil
}
