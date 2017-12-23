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

func (s *shard) getProgress(serverAdminAddress string) (segment uint32, offset uint64, hasProgress bool, err error) {

	segmentKey := []byte(fmt.Sprintf("%s.%d.next.segment", serverAdminAddress, s.id))
	offsetKey := []byte(fmt.Sprintf("%s.%d.next.offset", serverAdminAddress, s.id))

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

func (s *shard) setProgress(serverAdminAddress string, segment uint32, offset uint64) (err error) {

	log.Printf("shard %s follow server %v next segment %d offset %d", s, serverAdminAddress, segment, offset)

	segmentKey := []byte(fmt.Sprintf("%s.%d.next.segment", serverAdminAddress, s.id))
	offsetKey := []byte(fmt.Sprintf("%s.%d.next.offset", serverAdminAddress, s.id))

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
