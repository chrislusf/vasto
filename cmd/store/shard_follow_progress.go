package store

import (
	"context"
	"fmt"
	"github.com/chrislusf/glog"
	"github.com/chrislusf/vasto/topology"
	"github.com/chrislusf/vasto/util"
)

type progressKey struct {
	shardId            shard_id
	serverAdminAddress string
}

type progressValue struct {
	segment uint32
	offset  uint64
}

type followProcess struct {
	cancelFunc context.CancelFunc
}

var (
	INTERNAL_PREFIX = []byte("_vasto.")
)

func genSegmentOffsetKeys(serverAdminAddress string, shardId shard_id) (segmentKey []byte, offsetKey []byte) {
	segmentKey = []byte(fmt.Sprintf("_vasto.next.segment.%s.%d", serverAdminAddress, shardId))
	offsetKey = []byte(fmt.Sprintf("_vasto.next.offset.%s.%d", serverAdminAddress, shardId))
	return
}

// implementing PeriodicTask
func (s *shard) EverySecond() {
	// glog.V(2).Infof("%s every second", s)
	s.followProcessesLock.Lock()
	for pk, pv := range s.followProgress {
		if segment, offset, hasProgress, err := s.loadProgress(pk.serverAdminAddress, pk.shardId); err == nil {
			if !hasProgress || segment != pv.segment || offset != pv.offset {
				s.saveProgress(pk.serverAdminAddress, pk.shardId, pv.segment, pv.offset)
			}
		}
	}
	s.followProcessesLock.Unlock()
}

func (s *shard) loadProgress(serverAdminAddress string, targetShardId shard_id) (segment uint32, offset uint64, hasProgress bool, err error) {

	segmentKey, offsetKey := genSegmentOffsetKeys(serverAdminAddress, targetShardId)

	nextSegment := uint32(0)
	nextOffset := uint64(0)
	t, err := s.db.Get(segmentKey)
	if err == nil && len(t) > 0 {
		hasProgress = true
		nextSegment = util.BytesToUint32(t)
	}
	t, err = s.db.Get(offsetKey)
	if err == nil && len(t) > 0 {
		hasProgress = true
		nextOffset = util.BytesToUint64(t)
	}

	return nextSegment, nextOffset, hasProgress, err
}

func (s *shard) saveProgress(serverAdminAddress string, targetShardId shard_id, segment uint32, offset uint64) (err error) {

	glog.V(1).Infof("shard %s follow server %v shard %d next segment %d offset %d", s, serverAdminAddress, targetShardId, segment, offset)

	segmentKey, offsetKey := genSegmentOffsetKeys(serverAdminAddress, targetShardId)

	err = s.db.Put(segmentKey, util.Uint32toBytes(segment))
	if err != nil {
		return fmt.Errorf("setting %v = %d %d: %v", string(segmentKey), segment, offset, err)
	}
	err = s.db.Put(offsetKey, util.Uint64toBytes(offset))
	if err != nil {
		return fmt.Errorf("setting %v = %d %d: %v", string(offsetKey), segment, offset, err)
	}

	return nil
}

func (s *shard) clearProgress(serverAdminAddress string, targetShardId shard_id) {

	glog.V(1).Infof("shard %s stops following server %v.%d", s, serverAdminAddress, targetShardId)

	segmentKey, offsetKey := genSegmentOffsetKeys(serverAdminAddress, targetShardId)

	s.db.Delete(segmentKey)
	s.db.Delete(offsetKey)

}

func (s *shard) isFollowing(peer topology.ClusterShard) bool {
	s.followProcessesLock.Lock()
	_, found := s.followProcesses[peer]
	s.followProcessesLock.Unlock()
	return found
}

func (s *shard) startFollowProcess(peer topology.ClusterShard, cancelFunc context.CancelFunc) {
	s.followProcessesLock.Lock()
	s.followProcesses[peer] = &followProcess{cancelFunc: cancelFunc}
	s.followProcessesLock.Unlock()
}

func (s *shard) insertInMemoryFollowProgress(serverAdminAddress string, targetShardId shard_id, segment uint32, offset uint64) {
	s.followProgressLock.Lock()
	s.followProgress[progressKey{targetShardId, serverAdminAddress}] = progressValue{segment, offset}
	s.followProgressLock.Unlock()
}

func (s *shard) updateInMemoryFollowProgressIfPresent(serverAdminAddress string, targetShardId shard_id, segment uint32, offset uint64) (found bool) {
	s.followProgressLock.Lock()
	_, found = s.followProgress[progressKey{targetShardId, serverAdminAddress}]
	if found {
		s.followProgress[progressKey{targetShardId, serverAdminAddress}] = progressValue{segment, offset}
	}
	s.followProgressLock.Unlock()
	return found
}

func (s *shard) deleteInMemoryFollowProgress(serverAdminAddress string, targetShardId shard_id) {
	s.followProgressLock.Lock()
	delete(s.followProgress, progressKey{targetShardId, serverAdminAddress})
	s.followProgressLock.Unlock()
}
