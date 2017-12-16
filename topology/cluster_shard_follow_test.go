package topology

import (
	"testing"
	"github.com/stretchr/testify/assert"
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

}

func TestBootstrapPeersWhenShrinkingSmall(t *testing.T) {
	/*

	curr server id  : 0 1 2 3 4 5 6
	replica shard 0 : 0 1 2 3 4 5 6
	replica shard 1 : 6 0 1 2 3 4 5
	replica shard 2 : 5 6 0 1 2 3 4

	next server id  : 0 1 2 3 4 5
	replica shard 0 : 0 1 2 3 4 5
	replica shard 1 : 5 0 1 2 3 4
	replica shard 2 : 4 5 0 1 2 3

	*/

	shards := BootstrapPeersWhenResize(5, 4, 7, 6, 3)
	assert.Equal(t, 1, len(shards))
	assert.Equal(t, 6, shards[0].ServerId)

	shards = BootstrapPeersWhenResize(0, 4, 7, 6, 3)
	assert.Equal(t, 0, len(shards))

	shards = BootstrapPeersWhenResize(0, 5, 7, 6, 3)
	assert.Equal(t, 1, len(shards))
	assert.Equal(t, 6, shards[0].ServerId)

}

func TestBootstrapPeersWhenShrinkingBig(t *testing.T) {
	/*

	curr server id  : 0 1 2 3 4 5 6 7 8 9
	replica shard 0 : 0 1 2 3 4 5 6 7 8 9
	replica shard 1 : 9 0 1 2 3 4 5 6 7 8
	replica shard 2 : 8 9 0 1 2 3 4 5 6 7

	next server id  : 0 1 2 3 4 5
	replica shard 0 : 0 1 2 3 4 5
	replica shard 1 : 5 0 1 2 3 4
	replica shard 2 : 4 5 0 1 2 3

	*/

	shards := BootstrapPeersWhenResize(5, 4, 10, 6, 3)
	assert.Equal(t, 4, len(shards))
	assert.Equal(t, 6, shards[0].ServerId)
	assert.Equal(t, 7, shards[1].ServerId)
	assert.Equal(t, 8, shards[2].ServerId)
	assert.Equal(t, 9, shards[3].ServerId)

	shards = BootstrapPeersWhenResize(0, 4, 10, 6, 3)
	assert.Equal(t, 0, len(shards))

	shards = BootstrapPeersWhenResize(0, 8, 10, 6, 3)
	assert.Equal(t, 0, len(shards))

}

func TestBootstrapPeersWhenGrowingSmall(t *testing.T) {
	/*

	curr server id  : 0 1 2 3 4 5
	replica shard 0 : 0 1 2 3 4 5
	replica shard 1 : 5 0 1 2 3 4
	replica shard 2 : 4 5 0 1 2 3

	next server id  : 0 1 2 3 4 5 6
	replica shard 0 : 0 1 2 3 4 5 6
	replica shard 1 : 6 0 1 2 3 4 5
	replica shard 2 : 5 6 0 1 2 3 4

	*/

	shards := BootstrapPeersWhenResize(6, 6, 6, 7, 3)
	assert.Equal(t, 6, len(shards))
	assert.Equal(t, 0, shards[0].ServerId)
	assert.Equal(t, 1, shards[1].ServerId)
	assert.Equal(t, 2, shards[2].ServerId)
	assert.Equal(t, 3, shards[3].ServerId)
	assert.Equal(t, 4, shards[4].ServerId)
	assert.Equal(t, 5, shards[5].ServerId)

	shards = BootstrapPeersWhenResize(0, 6, 6, 7, 3)
	assert.Equal(t, 6, len(shards))
	assert.Equal(t, 0, shards[0].ServerId)
	assert.Equal(t, 1, shards[1].ServerId)
	assert.Equal(t, 2, shards[2].ServerId)
	assert.Equal(t, 3, shards[3].ServerId)
	assert.Equal(t, 4, shards[4].ServerId)
	assert.Equal(t, 5, shards[5].ServerId)

	shards = BootstrapPeersWhenResize(0, 5, 6, 7, 3)
	assert.Equal(t, 0, len(shards))

	shards = BootstrapPeersWhenResize(3, 2, 6, 7, 3)
	assert.Equal(t, 0, len(shards))

	shards = BootstrapPeersWhenResize(6, 4, 6, 7, 3)
	assert.Equal(t, 1, len(shards))
	assert.Equal(t, 4, shards[0].ServerId)

}

func TestBootstrapPeersWhenGrowingBig(t *testing.T) {
	/*

	curr server id  : 0 1 2 3 4 5
	replica shard 0 : 0 1 2 3 4 5
	replica shard 1 : 5 0 1 2 3 4
	replica shard 2 : 4 5 0 1 2 3

	next server id  : 0 1 2 3 4 5 6 7 8 9
	replica shard 0 : 0 1 2 3 4 5 6 7 8 9
	replica shard 1 : 9 0 1 2 3 4 5 6 7 8
	replica shard 2 : 8 9 0 1 2 3 4 5 6 7
	*/

	shards := BootstrapPeersWhenResize(6, 6, 6, 10, 3)
	assert.Equal(t, 6, len(shards))
	assert.Equal(t, 0, shards[0].ServerId)
	assert.Equal(t, 1, shards[1].ServerId)
	assert.Equal(t, 2, shards[2].ServerId)
	assert.Equal(t, 3, shards[3].ServerId)
	assert.Equal(t, 4, shards[4].ServerId)
	assert.Equal(t, 5, shards[5].ServerId)

	shards = BootstrapPeersWhenResize(0, 9, 6, 7, 3)
	assert.Equal(t, 6, len(shards))
	assert.Equal(t, 0, shards[0].ServerId)
	assert.Equal(t, 1, shards[1].ServerId)
	assert.Equal(t, 2, shards[2].ServerId)
	assert.Equal(t, 3, shards[3].ServerId)
	assert.Equal(t, 4, shards[4].ServerId)
	assert.Equal(t, 5, shards[5].ServerId)

	shards = BootstrapPeersWhenResize(7, 5, 6, 7, 3)
	assert.Equal(t, 1, len(shards))
	assert.Equal(t, 5, shards[0].ServerId)

	shards = BootstrapPeersWhenResize(3, 2, 6, 7, 3)
	assert.Equal(t, 0, len(shards))

}
