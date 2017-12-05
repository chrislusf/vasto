package shell

import (
	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/topology"
	"fmt"
)

func (s *shell) OnShardCreateEvent(cluster *topology.ClusterRing, resource *pb.StoreResource, shardStatus *pb.ShardStatus) {
	// fmt.Printf("cluster %v, but shell cluster %v", cluster, s.vastoClient.ClusterListener.GetClusterRing(shardStatus.KeyspaceName))
	if *s.option.DataCenter == resource.DataCenter && *s.option.Keyspace == shardStatus.KeyspaceName {
		fmt.Printf("\n+ node %d shard %d %s cluster %s\n> ", shardStatus.NodeId, shardStatus.ShardId, resource.Address, cluster)
	} else {
		fmt.Printf("\n+ dc %s keyspace %s node %d shard %d %s cluster %s\n> ", resource.DataCenter,
			shardStatus.KeyspaceName, shardStatus.NodeId, shardStatus.ShardId, resource.Address, cluster)
	}
}

func (s *shell) OnShardUpdateEvent(cluster *topology.ClusterRing, resource *pb.StoreResource, shardStatus *pb.ShardStatus, oldShardStatus *pb.ShardStatus) {
	if oldShardStatus == nil {
	} else if oldShardStatus.Status != shardStatus.Status {
		if *s.option.DataCenter == resource.DataCenter && *s.option.Keyspace == shardStatus.KeyspaceName {
			fmt.Printf("\n* node %d shard %d %s cluster %s status:%s=>%s\n> ",
				shardStatus.NodeId, shardStatus.ShardId, resource.GetAddress(), cluster,
				oldShardStatus.Status, shardStatus.Status)
		} else {
			fmt.Printf("\n* dc %s keyspace %s node %d shard %d %s cluster %s status:%s=>%s\n> ", resource.DataCenter,
				shardStatus.KeyspaceName, shardStatus.NodeId, shardStatus.ShardId, resource.GetAddress(), cluster,
				oldShardStatus.Status, shardStatus.Status)
		}
	}
}
func (s *shell) OnShardRemoveEvent(cluster *topology.ClusterRing, resource *pb.StoreResource, shardStatus *pb.ShardStatus) {
	if *s.option.DataCenter == resource.DataCenter && *s.option.Keyspace == shardStatus.KeyspaceName {
		fmt.Printf("\n- node %d shard %d %s cluster %s\n> ", shardStatus.NodeId, shardStatus.ShardId, resource.Address, cluster)
	} else {
		fmt.Printf("\n- dc %s keyspace %s node %d shard %d %s cluster %s\n> ", resource.DataCenter,
			shardStatus.KeyspaceName, shardStatus.NodeId, shardStatus.ShardId, resource.Address, cluster)
	}
}
