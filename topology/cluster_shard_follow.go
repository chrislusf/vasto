package topology

type ClusterShard struct {
	ShardId  int
	ServerId int
}

// PeerShards list peer shards that are on other cluster nodes
func PeerShards(selfServerId int, selfShardId int, clusterSize int, replicationFactor int) (peers []ClusterShard) {

	for i := 0; i < replicationFactor && i < clusterSize; i++ {
		serverId := selfShardId + i
		if serverId >= clusterSize {
			serverId -= clusterSize
		}
		if serverId == selfServerId {
			continue
		}
		peers = append(peers, ClusterShard{
			ShardId:  selfShardId,
			ServerId: serverId,
		})
	}

	return
}

// LocalShards list shards that local node should have
func LocalShards(selfServerId int, clusterSize int, replicationFactor int) (shards []ClusterShard) {

	for i := 0; i < replicationFactor && i < clusterSize; i++ {
		shardId := selfServerId - i
		if shardId < 0 {
			shardId += clusterSize
		}
		shards = append(shards, ClusterShard{
			ShardId:  shardId,
			ServerId: selfServerId,
		})
	}
	return
}

func isShardInLocal(shardId int, selfServerId int, clusterSize int, replicationFactor int) bool {
	shards := LocalShards(selfServerId, clusterSize, replicationFactor)
	for _, shard := range shards {
		if shardId == shard.ShardId {
			return true
		}
	}
	return false
}

func BootstrapPeersWhenResize(selfServerId int, selfShardId int, fromSize, toSize int, replicationFactor int) (bootstrapFollow []ClusterShard) {
	if fromSize == toSize {
		return PeerShards(selfServerId, selfShardId, toSize, replicationFactor)
	}
	if fromSize < toSize {
		// growing cluster
		if selfShardId >= fromSize {
			// new shards just follow all existing shards, with its own filter
			for i := 0; i < fromSize; i++ {
				bootstrapFollow = append(bootstrapFollow, ClusterShard{
					ShardId:  i,
					ServerId: i,
				})
			}
			return
		}
		// old shards
		if selfServerId >= fromSize {
			// if old shards and on new servers
			bootstrapFollow = append(bootstrapFollow, ClusterShard{
				ShardId:  selfShardId,
				ServerId: selfShardId,
			})
			return
		} else {
			// if old shards and on existing servers, nothing to bootstrap
			return
		}

	} else {
		// shrinking cluster
		if selfServerId >= toSize {
			// if retiring servers, nothing to bootstrap
			return
		}
		if selfShardId >= toSize {
			// if retiring shards, nothing to bootstrap
			return
		}
		if isShardInLocal(selfShardId, selfServerId, toSize, replicationFactor) {
			// local in new cluster
			if !isShardInLocal(selfShardId, selfServerId, fromSize, replicationFactor){
				// does not exist before the new cluster
				// skip, will create the shard after new cluster settles
				return
			} else {
				// already exists, in both new and old cluster
				// add copying from the retiring servers
				for i := toSize; i < fromSize; i++ {
					bootstrapFollow = append(bootstrapFollow, ClusterShard{
						ShardId:  i,
						ServerId: i,
					})
				}
				return
			}
		} else {
			// not local in new cluster
			// moving out, nothing to do
			return
		}

	}
}
