package store

import "sync"

type keyspace_name string

type keyspaceShards struct {
	keyspaceToShards map[keyspace_name][]*node
	sync.RWMutex
}

func newKeyspaceShards() *keyspaceShards {
	return &keyspaceShards{
		keyspaceToShards: make(map[keyspace_name][]*node),
	}
}

func (ks *keyspaceShards) getShards(keyspaceName string) []*node {
	ks.RLock()
	t := ks.keyspaceToShards[keyspace_name(keyspaceName)]
	ks.RUnlock()
	return t
}

func (ks *keyspaceShards) addShards(keyspaceName string, nodes ...*node) {
	ks.Lock()
	if _, found := ks.keyspaceToShards[keyspace_name(keyspaceName)]; found {
		ks.keyspaceToShards[keyspace_name(keyspaceName)] = append(ks.keyspaceToShards[keyspace_name(keyspaceName)], nodes...)
	} else {
		ks.keyspaceToShards[keyspace_name(keyspaceName)] = nodes
	}
	ks.Unlock()
}

func (ks *keyspaceShards) deleteKeyspace(keyspaceName string) {
	ks.Lock()
	delete(ks.keyspaceToShards, keyspace_name(keyspaceName))
	ks.Unlock()
}
