package rocks

import (
	"github.com/cespare/xxhash"
	"github.com/dgryski/go-jump"
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
	jumpHash := jump.Hash(xxhash.Sum64(key), m.shardCount)
	if m.shardId == jumpHash {
		return false, val
	}
	return true, val
}

func (d *rocks) SetCompactionForShard(shardId, shardCount int) {
	d.compactionFilter.configure(int32(shardId), shardCount)
}
