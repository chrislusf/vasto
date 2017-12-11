package store

import (
	"fmt"
	"github.com/chrislusf/vasto/util"
	"log"
)

func (s *shard) getProgress() (segment uint32, offset uint64, hasProgress bool, err error) {

	nextSegment := uint32(0)
	nextOffset := uint64(0)
	t, err := s.db.Get(s.nextSegmentKey)
	if err == nil && len(t) > 0 {
		hasProgress = true
		nextSegment = util.BytesToUint32(t)
	}
	t, err = s.db.Get(s.nextOffsetKey)
	if err == nil && len(t) > 0 {
		hasProgress = true
		nextOffset = util.BytesToUint64(t)
	}

	return nextSegment, nextOffset, hasProgress, err
}

func (s *shard) setProgress(segment uint32, offset uint64) (err error) {

	log.Printf("shard %s saves next segment %d offset %d", s, segment, offset)

	err = s.db.Put(s.nextSegmentKey, util.Uint32toBytes(segment))
	if err != nil {
		return fmt.Errorf("shard %s sets follow progress: segment %d %d: %v", s, segment, offset, err)
	}
	err = s.db.Put(s.nextOffsetKey, util.Uint64toBytes(offset))
	if err != nil {
		return fmt.Errorf("shard %s sets follow progress: offset %d %d: %v", s, segment, offset, err)
	}

	return nil
}
