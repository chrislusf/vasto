package rocks

import (
	"github.com/chrislusf/vasto/storage/codec"
	"github.com/dgryski/go-jump"
	"time"
)

type shardingCompactionFilter struct {
	shardId    int32
	shardCount int
}

func (m *shardingCompactionFilter) configure(shardId int32, shardCount int) {
	m.shardId = shardId
	m.shardCount = shardCount
}

func (m *shardingCompactionFilter) Name() string { return "vasto.sharding" }
func (m *shardingCompactionFilter) Filter(level int, key, val []byte) (bool, []byte) {
	entry := codec.FromBytes(val)
	jumpHash := jump.Hash(entry.PartitionHash, m.shardCount)
	if m.shardId != jumpHash {
		return true, nil
	}
	if entry.TtlSecond == 0 {
		return false, nil
	}
	if entry.UpdatedAtNs/uint64(1000000)+uint64(entry.TtlSecond) < uint64(time.Now().Unix()) {
		return true, nil
	}
	return false, nil
}

// SetCompactionForShard changes the compaction filter to use the shardId and shardCount.
// All entries not belong to the shard will be physically purged during next compaction.
func (d *Rocks) SetCompactionForShard(shardId, shardCount int) {
	d.compactionFilter.configure(int32(shardId), shardCount)
}
