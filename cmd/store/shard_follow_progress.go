package store

import (
	"fmt"
	"github.com/chrislusf/vasto/util"
	"log"
)

type progressKey struct {
	shardId  shard_id
	serverId server_id
}
type progressValue struct {
	segment uint32
	offset  uint64
}

func (s *shard) getProgress(serverId server_id) (segment uint32, offset uint64, hasProgress bool, err error) {

	segmentKey := []byte(fmt.Sprintf("%d.%d.next.segment", s.id, serverId))
	offsetKey := []byte(fmt.Sprintf("%d.%d.next.offset", s.id, serverId))

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

func (s *shard) setProgress(serverId server_id, segment uint32, offset uint64) (err error) {

	log.Printf("shard %s follow server %d next segment %d offset %d", s, serverId, segment, offset)

	segmentKey := []byte(fmt.Sprintf("%d.%d.next.segment", s.id, serverId))
	offsetKey := []byte(fmt.Sprintf("%d.%d.next.offset", s.id, serverId))

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
