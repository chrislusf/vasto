package store

import (
	"sort"
	"sync"
)

type keyspaceName string

type keyspaceShards struct {
	keyspaceToShards map[keyspaceName][]*shard
	sync.RWMutex
}

func newKeyspaceShards() *keyspaceShards {
	return &keyspaceShards{
		keyspaceToShards: make(map[keyspaceName][]*shard),
	}
}

func (ks *keyspaceShards) getShards(ksName string) (shards []*shard, found bool) {
	ks.RLock()
	shards, found = ks.keyspaceToShards[keyspaceName(ksName)]
	ks.RUnlock()
	return
}

func (ks *keyspaceShards) getShard(keyspaceName string, shardId VastoShardId) (shard *shard, found bool) {
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

func (ks *keyspaceShards) addShards(ksName string, nodes ...*shard) {
	ks.Lock()
	shards := ks.keyspaceToShards[keyspaceName(ksName)]
	if _, found := ks.keyspaceToShards[keyspaceName(ksName)]; found {
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
	ks.keyspaceToShards[keyspaceName(ksName)] = shards
	ks.Unlock()
}

func (ks *keyspaceShards) deleteKeyspace(ksName string) {
	ks.Lock()
	delete(ks.keyspaceToShards, keyspaceName(ksName))
	ks.Unlock()
}

func (ks *keyspaceShards) removeShard(node *shard) {
	ks.Lock()

	shards := ks.keyspaceToShards[keyspaceName(node.keyspace)]
	var t []*shard
	for _, shard := range shards {
		if shard.id != node.id {
			t = append(t, shard)
		}
	}

	ks.keyspaceToShards[keyspaceName(node.keyspace)] = t
	ks.Unlock()
}
