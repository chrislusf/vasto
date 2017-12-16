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
