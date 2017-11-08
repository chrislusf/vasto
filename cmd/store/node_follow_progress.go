package store

import (
	"fmt"
	"github.com/chrislusf/vasto/util"
)

func (n *node) getProgress() (segment uint32, offset uint64, err error) {

	nextSegment := uint32(0)
	nextOffset := uint64(0)
	t, err := n.db.Get(n.nextSegmentKey)
	if err == nil && len(t) > 0 {
		nextSegment = util.BytesToUint32(t)
	}
	t, err = n.db.Get(n.nextOffsetKey)
	if err == nil && len(t) > 0 {
		nextOffset = util.BytesToUint64(t)
	}

	return nextSegment, nextOffset, err
}

func (n *node) setProgress(segment uint32, offset uint64) (err error) {

	// println("Saving next segment", segment, "offset", offset)

	err = n.db.Put(n.nextSegmentKey, util.Uint32toBytes(segment))
	if err != nil {
		return fmt.Errorf("set follow progress: segment %d %d: %v", segment, offset, err)
	}
	n.db.Put(n.nextOffsetKey, util.Uint64toBytes(offset))
	if err != nil {
		return fmt.Errorf("set follow progress: offset %d %d: %v", segment, offset, err)
	}

	return nil
}
