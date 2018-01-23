package rocks

import (
	"github.com/chrislusf/vasto/util"
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
	jumpHash := jump.Hash(util.Hash(key), m.shardCount)
	if m.shardId == jumpHash {
		return false, val
	}
	return true, val
}

func (d *Rocks) SetCompactionForShard(shardId, shardCount int) {
	d.compactionFilter.configure(int32(shardId), shardCount)
}
