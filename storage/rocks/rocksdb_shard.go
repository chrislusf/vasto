package rocks

import (
	"github.com/chrislusf/glog"
	"github.com/chrislusf/gorocksdb"
	"github.com/chrislusf/vasto/storage/codec"
	"github.com/dgryski/go-jump"
	"time"
)

type shardingCompactionFilter struct {
	shardId    int32
	shardCount int
	isResizing bool
}

func (m *shardingCompactionFilter) configure(shardId int32, shardCount int) {
	m.shardId = shardId
	m.shardCount = shardCount
}

func (m *shardingCompactionFilter) Name() string { return "vasto.sharding" }
func (m *shardingCompactionFilter) Filter(level int, key, val []byte) (bool, []byte) {
	entry := codec.FromBytes(val)
	if entry == nil {
		// vasto specific entries not encoded into Entry
		glog.V(1).Infof("vasto internal %s: %s", string(key), string(val))
		return false, nil
	}
	if !m.isResizing {
		// do not delete anything if during resizing, in case the resizing fails
		jumpHash := jump.Hash(entry.PartitionHash, m.shardCount)
		if m.shardId != jumpHash {
			// glog.V(1).Infof("skipping shard %d, jumpHash %d, shardCount:%d, skipping %s: %s", m.shardId, jumpHash, m.shardCount, string(key), string(val))
			return true, nil
		}
	}
	if entry.TtlSecond == 0 {
		return false, nil
	}
	if entry.UpdatedAtNs/uint64(1000000)+uint64(entry.TtlSecond) < uint64(time.Now().Unix()) {
		// glog.V(1).Infof("skipping updatedAt:%d, ttl:%d", entry.UpdatedAtNs/uint64(1000000), entry.TtlSecond, string(key), string(val))
		return true, nil
	}
	return false, nil
}

// SetCompactionForShard changes the compaction filter to use the shardId and shardCount.
// All entries not belong to the shard will be physically purged during next compaction.
func (d *Rocks) SetCompactionForShard(shardId, shardCount int) {
	d.compactionFilter.configure(int32(shardId), shardCount)
}

func (d *Rocks) PrepareForClusterResize() {
	d.compactionFilter.isResizing = true
}

func (d *Rocks) CompleteClusterResize() {
	d.compactionFilter.isResizing = false
}

func (d *Rocks) Compact() {
	d.db.Flush(gorocksdb.NewDefaultFlushOptions())
	d.db.CompactRange(gorocksdb.Range{Start: nil, Limit: nil})
}
