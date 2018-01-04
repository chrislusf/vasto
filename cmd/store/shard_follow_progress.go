package store

import (
	"fmt"
	"github.com/chrislusf/vasto/util"
	"log"
)

type progressKey struct {
	shardId            shard_id
	serverAdminAddress string
}
type progressValue struct {
	segment uint32
	offset  uint64
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
	// log.Printf("%s every second", s)
	s.followProgressLock.Lock()
	for pk, pv := range s.followProgress {
		if segment, offset, hasProgress, err := s.loadProgress(pk.serverAdminAddress); err == nil {
			if !hasProgress || segment != pv.segment || offset != pv.offset {
				s.saveProgress(pk.serverAdminAddress, pv.segment, pv.offset)
			}
		}
	}
	s.followProgressLock.Unlock()
}

func (s *shard) loadProgress(serverAdminAddress string) (segment uint32, offset uint64, hasProgress bool, err error) {

	segmentKey, offsetKey := genSegmentOffsetKeys(serverAdminAddress, s.id)

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

func (s *shard) saveProgress(serverAdminAddress string, segment uint32, offset uint64) (err error) {

	log.Printf("shard %s follow server %v next segment %d offset %d", s, serverAdminAddress, segment, offset)

	segmentKey, offsetKey := genSegmentOffsetKeys(serverAdminAddress, s.id)

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

func (s *shard) clearProgress(serverAdminAddress string) {

	log.Printf("shard %s stops following server %v", s, serverAdminAddress)

	segmentKey, offsetKey := genSegmentOffsetKeys(serverAdminAddress, s.id)

	s.db.Delete(segmentKey)
	s.db.Delete(offsetKey)

}

func (s *shard) insertInMemoryFollowProgress(serverAdminAddress string, segment uint32, offset uint64) {
	s.followProgressLock.Lock()
	s.followProgress[progressKey{s.id, serverAdminAddress}] = progressValue{segment, offset}
	s.followProgressLock.Unlock()
}

func (s *shard) updateInMemoryFollowProgressIfPresent(serverAdminAddress string, segment uint32, offset uint64) (found bool) {
	s.followProgressLock.Lock()
	_, found = s.followProgress[progressKey{s.id, serverAdminAddress}]
	if found {
		s.followProgress[progressKey{s.id, serverAdminAddress}] = progressValue{segment, offset}
	}
	s.followProgressLock.Unlock()
	return found
}

func (s *shard) deleteInMemoryFollowProgress(serverAdminAddress string) {
	s.followProgressLock.Lock()
	delete(s.followProgress, progressKey{s.id, serverAdminAddress})
	s.followProgressLock.Unlock()
}
