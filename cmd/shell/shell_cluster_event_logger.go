package shell

import (
	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/topology"
	"fmt"
)

func (s *shell) OnShardCreateEvent(cluster *topology.ClusterRing, resource *pb.StoreResource, ShardInfo *pb.ShardInfo) {
	// fmt.Printf("cluster %v, but shell cluster %v", cluster, s.vastoClient.ClusterListener.GetClusterRing(ShardInfo.KeyspaceName))
	if *s.option.DataCenter == resource.DataCenter && *s.option.Keyspace == ShardInfo.KeyspaceName {
		fmt.Printf("\n+ node %d shard %d %s cluster %s\n> ", ShardInfo.ServerId, ShardInfo.ShardId, resource.Address, cluster)
	} else {
		fmt.Printf("\n+ dc %s keyspace %s node %d shard %d %s cluster %s\n> ", resource.DataCenter,
			ShardInfo.KeyspaceName, ShardInfo.ServerId, ShardInfo.ShardId, resource.Address, cluster)
	}
}

func (s *shell) OnShardUpdateEvent(cluster *topology.ClusterRing, resource *pb.StoreResource, ShardInfo *pb.ShardInfo, oldShardInfo *pb.ShardInfo) {
	if oldShardInfo == nil {
	} else if oldShardInfo.Status != ShardInfo.Status {
		if *s.option.DataCenter == resource.DataCenter && *s.option.Keyspace == ShardInfo.KeyspaceName {
			fmt.Printf("\n* node %d shard %d %s cluster %s status:%s=>%s\n> ",
				ShardInfo.ServerId, ShardInfo.ShardId, resource.GetAddress(), cluster,
				oldShardInfo.Status, ShardInfo.Status)
		} else {
			fmt.Printf("\n* dc %s keyspace %s node %d shard %d %s cluster %s status:%s=>%s\n> ", resource.DataCenter,
				ShardInfo.KeyspaceName, ShardInfo.ServerId, ShardInfo.ShardId, resource.GetAddress(), cluster,
				oldShardInfo.Status, ShardInfo.Status)
		}
	}
}

func (s *shell) OnShardRemoveEvent(cluster *topology.ClusterRing, resource *pb.StoreResource, ShardInfo *pb.ShardInfo) {
	if *s.option.DataCenter == resource.DataCenter && *s.option.Keyspace == ShardInfo.KeyspaceName {
		fmt.Printf("\n- node %d shard %d %s cluster %s\n> ", ShardInfo.ServerId, ShardInfo.ShardId, resource.Address, cluster)
	} else {
		fmt.Printf("\n- dc %s keyspace %s node %d shard %d %s cluster %s\n> ", resource.DataCenter,
			ShardInfo.KeyspaceName, ShardInfo.ServerId, ShardInfo.ShardId, resource.Address, cluster)
	}
}

func (s *shell) OnShardPromoteEvent(cluster *topology.ClusterRing, resource *pb.StoreResource, ShardInfo *pb.ShardInfo) {
	if *s.option.DataCenter == resource.DataCenter && *s.option.Keyspace == ShardInfo.KeyspaceName {
		fmt.Printf("\n=> node %d shard %d %s cluster %s\n> ", ShardInfo.ServerId, ShardInfo.ShardId, resource.Address, cluster)
	} else {
		fmt.Printf("\n=> dc %s keyspace %s node %d shard %d %s cluster %s\n> ", resource.DataCenter,
			ShardInfo.KeyspaceName, ShardInfo.ServerId, ShardInfo.ShardId, resource.Address, cluster)
	}
}
