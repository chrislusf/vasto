package topology

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPeerShards(t *testing.T) {

	// first server, main shard
	peers := PeerShards(0, 0, 7, 3)
	assert.Equal(t, 2, len(peers))
	assert.Equal(t, 1, peers[0].ServerId)
	assert.Equal(t, 2, peers[1].ServerId)

	// first server, second shard
	peers = PeerShards(0, 6, 7, 3)
	assert.Equal(t, 2, len(peers))
	assert.Equal(t, 6, peers[0].ServerId)
	assert.Equal(t, 1, peers[1].ServerId)

	// second server, third shard
	peers = PeerShards(1, 6, 7, 3)
	assert.Equal(t, 2, len(peers))
	assert.Equal(t, 6, peers[0].ServerId)
	assert.Equal(t, 0, peers[1].ServerId)

	// last server, main shard
	peers = PeerShards(6, 6, 7, 3)
	assert.Equal(t, 2, len(peers))
	assert.Equal(t, 0, peers[0].ServerId)
	assert.Equal(t, 1, peers[1].ServerId)

	// last server, second shard
	peers = PeerShards(6, 5, 7, 3)
	assert.Equal(t, 2, len(peers))
	assert.Equal(t, 5, peers[0].ServerId)
	assert.Equal(t, 0, peers[1].ServerId)

	// second server, in 2 servers 1 replica
	peers = PeerShards(1, 1, 2, 1)
	assert.Equal(t, 0, len(peers))

	// second server, in existing 1 servers 1 replica
	peers = PeerShards(1, 1, 1, 1)
	assert.Equal(t, 0, len(peers))

}

func TestLocalShards(t *testing.T) {

	// last server
	shards := LocalShards(6, 7, 3)
	assert.Equal(t, 3, len(shards))
	assert.Equal(t, 6, shards[0].ShardId)
	assert.Equal(t, 5, shards[1].ShardId)
	assert.Equal(t, 4, shards[2].ShardId)

	// first server
	shards = LocalShards(0, 7, 3)
	assert.Equal(t, 3, len(shards))
	assert.Equal(t, 0, shards[0].ShardId)
	assert.Equal(t, 6, shards[1].ShardId)
	assert.Equal(t, 5, shards[2].ShardId)

	// second server
	shards = LocalShards(1, 7, 3)
	assert.Equal(t, 3, len(shards))
	assert.Equal(t, 1, shards[0].ShardId)
	assert.Equal(t, 0, shards[1].ShardId)
	assert.Equal(t, 6, shards[2].ShardId)

	// 2 nodes, first server
	shards = LocalShards(0, 2, 2)
	assert.Equal(t, 2, len(shards))
	assert.Equal(t, 0, shards[0].ShardId)
	assert.Equal(t, 1, shards[1].ShardId)

	// 2 nodes, second server
	shards = LocalShards(1, 2, 2)
	assert.Equal(t, 2, len(shards))
	assert.Equal(t, 1, shards[0].ShardId)
	assert.Equal(t, 0, shards[1].ShardId)

	// 1 node, the server
	shards = LocalShards(0, 1, 3)
	assert.Equal(t, 1, len(shards))
	assert.Equal(t, 0, shards[0].ShardId)

	// 3 nodes
	shards = LocalShards(2, 3, 2)
	assert.Equal(t, 2, len(shards))
	assert.Equal(t, 2, shards[0].ShardId)
	assert.Equal(t, 1, shards[1].ShardId)

}

func TestShardListContains(t *testing.T) {

	shards := LocalShards(6, 7, 3)

	assert.Equal(t, ShardListContains(shards, ClusterShard{
		ShardId:  6,
		ServerId: 6,
	}), true, "server 6 shard 6")

	assert.Equal(t, ShardListContains(shards, ClusterShard{
		ShardId:  5,
		ServerId: 6,
	}), true, "server 6 shard 5")

	assert.Equal(t, ShardListContains(shards, ClusterShard{
		ShardId:  4,
		ServerId: 6,
	}), true, "server 6 shard 4")

	assert.Equal(t, ShardListContains(shards, ClusterShard{
		ShardId:  2,
		ServerId: 6,
	}), false, "server 6 shard 2")

}
