package shell

import (
	"fmt"
	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/topology"
)

func (s *shell) OnShardCreateEvent(cluster *topology.Cluster, resource *pb.StoreResource, shardInfo *pb.ShardInfo) {
	// fmt.Printf("cluster %v, but shell cluster %v", cluster, s.vastoClient.ClusterListener.GetClusterRing(shardInfo.KeyspaceName))
	if *s.option.Keyspace == shardInfo.KeyspaceName {
		fmt.Printf("\n+ server %d shard %d %s cluster %s\n> ", shardInfo.ServerId, shardInfo.ShardId, resource.Address, cluster)
	} else {
		fmt.Printf("\n+ %s %s cluster %s\n> ",
			shardInfo.IdentifierOnThisServer(), resource.Address, cluster)
	}
}

func (s *shell) OnShardUpdateEvent(cluster *topology.Cluster, resource *pb.StoreResource, shardInfo *pb.ShardInfo, oldShardInfo *pb.ShardInfo) {
	if oldShardInfo == nil {
	} else if oldShardInfo.Status != shardInfo.Status {
		if *s.option.Keyspace == shardInfo.KeyspaceName {
			fmt.Printf("\n* server %d shard %d %s cluster %s status:%s=>%s\n> ",
				shardInfo.ServerId, shardInfo.ShardId, resource.GetAddress(), cluster,
				oldShardInfo.Status, shardInfo.Status)
		} else {
			fmt.Printf("\n* %s %s cluster %s status:%s=>%s\n> ",
				shardInfo.IdentifierOnThisServer(), resource.GetAddress(), cluster,
				oldShardInfo.Status, shardInfo.Status)
		}
	}
}

func (s *shell) OnShardRemoveEvent(cluster *topology.Cluster, resource *pb.StoreResource, shardInfo *pb.ShardInfo) {
	if *s.option.Keyspace == shardInfo.KeyspaceName {
		fmt.Printf("\n- server %d shard %d %s cluster %s\n> ", shardInfo.ServerId, shardInfo.ShardId, resource.Address, cluster)
	} else {
		fmt.Printf("\n- %s %s cluster %s\n> ",
			shardInfo.IdentifierOnThisServer(), resource.Address, cluster)
	}
}

func (s *shell) OnShardPromoteEvent(cluster *topology.Cluster, resource *pb.StoreResource, shardInfo *pb.ShardInfo) {
	if *s.option.Keyspace == shardInfo.KeyspaceName {
		fmt.Printf("\n=> server %d shard %d %s cluster %s\n> ", shardInfo.ServerId, shardInfo.ShardId, resource.Address, cluster)
	} else {
		fmt.Printf("\n=> %s %s cluster %s\n> ",
			shardInfo.IdentifierOnThisServer(), resource.Address, cluster)
	}
}
