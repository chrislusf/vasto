package store

import (
	"fmt"
	"github.com/chrislusf/vasto/util"
	"log"
)

func (n *node) getProgress() (segment uint32, offset uint64, hasProgress bool, err error) {

	nextSegment := uint32(0)
	nextOffset := uint64(0)
	t, err := n.db.Get(n.nextSegmentKey)
	if err == nil && len(t) > 0 {
		hasProgress = true
		nextSegment = util.BytesToUint32(t)
	}
	t, err = n.db.Get(n.nextOffsetKey)
	if err == nil && len(t) > 0 {
		hasProgress = true
		nextOffset = util.BytesToUint64(t)
	}

	return nextSegment, nextOffset, hasProgress, err
}

func (n *node) setProgress(segment uint32, offset uint64) (err error) {

	log.Printf("shard %s saves next segment %d offset %d", n, segment, offset)

	err = n.db.Put(n.nextSegmentKey, util.Uint32toBytes(segment))
	if err != nil {
		return fmt.Errorf("shard %s sets follow progress: segment %d %d: %v", n, segment, offset, err)
	}
	err = n.db.Put(n.nextOffsetKey, util.Uint64toBytes(offset))
	if err != nil {
		return fmt.Errorf("shard %s sets follow progress: offset %d %d: %v", n, segment, offset, err)
	}

	return nil
}
