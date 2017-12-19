package store

import (
	"github.com/chrislusf/vasto/topology"
	"github.com/chrislusf/vasto/pb"
	"log"
)

// the following functions implements cluster_listener.ShardEventProcessor

func (s *shard) OnShardCreateEvent(cluster *topology.ClusterRing, resource *pb.StoreResource, shardInfo *pb.ShardInfo) {

	if s.keyspace != shardInfo.KeyspaceName {
		return
	}

	if shardInfo.IsCandidate {
		if int(s.id) == int(shardInfo.ShardId) {
			log.Printf("+ shard %v found new candidate shard %s", s.String(), shardInfo.IdentifierOnThisServer())
		} else {
		}
		return
	} else {
		if int(s.id) == int(shardInfo.ShardId) {
		} else {
			log.Printf("+ shard %v found peer shard %v", s.String(), shardInfo.IdentifierOnThisServer())
		}
	}
}

func (s *shard) OnShardUpdateEvent(cluster *topology.ClusterRing, resource *pb.StoreResource, shardInfo *pb.ShardInfo, oldShardInfo *pb.ShardInfo) {

	if s.keyspace != shardInfo.KeyspaceName {
		return
	}

	if shardInfo.IsCandidate {
		if int(s.id) == int(shardInfo.ShardId) {
			log.Printf("~ shard %v found updated candidate shard %s", s.String(), shardInfo.IdentifierOnThisServer())
		} else {
			log.Printf("~ shard %v found updated candidate peer shard %s", s.String(), shardInfo.IdentifierOnThisServer())
		}
		return
	} else {
		if oldShardInfo == nil {
		} else if oldShardInfo.Status != shardInfo.Status {
			log.Printf("~ shard %s found updated shard %v cluster %s status:%s=>%s",
				s.String(), shardInfo.IdentifierOnThisServer(), cluster, oldShardInfo.Status, shardInfo.Status)
		}
		if int(s.id) == int(shardInfo.ShardId) {
		} else {
		}
	}

}
func (s *shard) OnShardRemoveEvent(cluster *topology.ClusterRing, resource *pb.StoreResource, shardInfo *pb.ShardInfo) {

	if s.keyspace != shardInfo.KeyspaceName {
		return
	}

	if shardInfo.IsCandidate {
		if int(s.id) == int(shardInfo.ShardId) {
			log.Printf("- shard %v removed new candidate shard %s", s.String(), shardInfo.IdentifierOnThisServer())
		} else {
		}
		return
	} else {
		if int(s.id) == int(shardInfo.ShardId) {
			log.Printf("- shard %v removed shard %v from cluster %s", s.String(), shardInfo.IdentifierOnThisServer(), cluster)
		} else {
		}
	}

}
