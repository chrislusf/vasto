package store

import (
	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/topology"
	"log"
)

// the following functions implements cluster_listener.ShardEventProcessor

func (s *shard) OnShardCreateEvent(cluster *topology.Cluster, resource *pb.StoreResource, shardInfo *pb.ShardInfo) {

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

func (s *shard) OnShardUpdateEvent(cluster *topology.Cluster, resource *pb.StoreResource, shardInfo *pb.ShardInfo, oldShardInfo *pb.ShardInfo) {

	if s.keyspace != shardInfo.KeyspaceName {
		return
	}

	if shardInfo.IsCandidate {
		if int(s.id) == int(shardInfo.ShardId) {
			log.Printf("~ found updated candidate shard %s", shardInfo.IdentifierOnThisServer())
		} else {
			log.Printf("~ found updated candidate peer shard %s", shardInfo.IdentifierOnThisServer())
		}
		return
	} else {
		if oldShardInfo == nil {
		} else if oldShardInfo.Status != shardInfo.Status {
			log.Printf("~ found updated shard %v cluster %s status:%s=>%s",
				shardInfo.IdentifierOnThisServer(), cluster, oldShardInfo.Status, shardInfo.Status)
		}
		if int(s.id) == int(shardInfo.ShardId) {
		} else {
		}
	}

}

func (s *shard) OnShardRemoveEvent(cluster *topology.Cluster, resource *pb.StoreResource, shardInfo *pb.ShardInfo) {

	if s.keyspace != shardInfo.KeyspaceName {
		return
	}

	if shardInfo.IsCandidate {
		if int(s.id) == int(shardInfo.ShardId) {
			log.Printf("- removed candidate shard %s", shardInfo.IdentifierOnThisServer())
		} else {
		}
		return
	} else {
		if int(s.id) == int(shardInfo.ShardId) {
		} else {
		}

		log.Printf("- removed shard %v from cluster %s", shardInfo.IdentifierOnThisServer(), cluster)

		if shardInfo.IsPermanentDelete {
			// delete from in memory progress and on disk progress, if exists
			s.deleteInMemoryFollowProgress(resource.GetAdminAddress(), shard_id(shardInfo.ShardId))

			if server_id(shardInfo.ServerId) != s.serverId {
				s.clearProgress(resource.GetAdminAddress(), shard_id(shardInfo.ShardId))
			}
		}

	}

}

func (s *shard) OnShardPromoteEvent(cluster *topology.Cluster, resource *pb.StoreResource, shardInfo *pb.ShardInfo) {

	if s.keyspace != shardInfo.KeyspaceName {
		return
	}

	if int(s.id) == int(shardInfo.ShardId) {
		log.Printf("=> shard %v promoted in cluster %s", shardInfo.IdentifierOnThisServer(), cluster)
	} else {
	}

}
