package store

import (
	"sync"
	"sort"
)

type keyspace_name string

type keyspaceShards struct {
	keyspaceToShards map[keyspace_name][]*shard
	sync.RWMutex
}

func newKeyspaceShards() *keyspaceShards {
	return &keyspaceShards{
		keyspaceToShards: make(map[keyspace_name][]*shard),
	}
}

func (ks *keyspaceShards) getShards(keyspaceName string) (shards []*shard, found bool) {
	ks.RLock()
	shards, found = ks.keyspaceToShards[keyspace_name(keyspaceName)]
	ks.RUnlock()
	return
}

func (ks *keyspaceShards) getShard(keyspaceName string, shardId shard_id) (shard *shard, found bool) {
	shards, hasShards := ks.getShards(keyspaceName)
	if !hasShards {
		return
	}

	for _, shard := range shards {
		if shard.id == shardId {
			return shard, true
		}
	}
	return
}

func (ks *keyspaceShards) addShards(keyspaceName string, nodes ...*shard) {
	ks.Lock()
	shards := ks.keyspaceToShards[keyspace_name(keyspaceName)]
	if _, found := ks.keyspaceToShards[keyspace_name(keyspaceName)]; found {
		shards = append(shards, nodes...)
	} else {
		shards = nodes
	}
	// sort the shards so that the primary shard is the first, and secondary shard is the second, etc.
	sort.Slice(shards, func(i, j int) bool {
		x := int(shards[i].serverId) - int(shards[i].id)
		if x < 0 {
			x += len(shards)
		}
		y := int(shards[j].serverId) - int(shards[j].id)
		if y < 0 {
			y += len(shards)
		}
		return x < y
	})
	ks.keyspaceToShards[keyspace_name(keyspaceName)] = shards
	ks.Unlock()
}

func (ks *keyspaceShards) deleteKeyspace(keyspaceName string) {
	ks.Lock()
	delete(ks.keyspaceToShards, keyspace_name(keyspaceName))
	ks.Unlock()
}

func (ks *keyspaceShards) removeShard(node *shard) {
	ks.Lock()

	shards := ks.keyspaceToShards[keyspace_name(node.keyspace)]
	var t []*shard
	for _, shard := range shards {
		if shard.id != node.id {
			t = append(t, shard)
		}
	}

	ks.keyspaceToShards[keyspace_name(node.keyspace)] = t
	ks.Unlock()
}
