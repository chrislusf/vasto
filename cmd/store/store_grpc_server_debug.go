package store

import (
	"github.com/chrislusf/vasto/pb"
	"golang.org/x/net/context"
	"fmt"
)

func (ss *storeServer) DebugStore(ctx context.Context, request *pb.Empty) (*pb.Empty, error) {

	resp := &pb.Empty{}

	ss.debug()

	return resp, nil

}

func (ss *storeServer) debug() () {

	ss.statusInClusterLock.RLock()
	fmt.Println("\n========================================================")
	fmt.Printf("local shards:\n")
	for keyspace, localShards := range ss.statusInCluster {
		fmt.Printf("  * %s server:%d, clusterSize:%d replicationFactor:%d\n",
			keyspace, localShards.Id, localShards.ClusterSize, localShards.ReplicationFactor)
		for _, shardInfo := range localShards.ShardMap {
			fmt.Printf("      * %+v clusterSize:%d replicationFactor:%d isCandidate:%v\n",
				shardInfo.IdentifierOnThisServer(), shardInfo.ClusterSize, shardInfo.ReplicationFactor, shardInfo.IsCandidate)
		}
	}
	ss.statusInClusterLock.RUnlock()

	fmt.Printf("\nperiodic tasks:\n")
	for _, task := range ss.periodTasks {
		fmt.Printf("  * %v\n", task)
	}

	ss.keyspaceShards.RLock()
	fmt.Printf("\nkeyspace shards:\n")
	for keyspaceName, shards := range ss.keyspaceShards.keyspaceToShards {
		fmt.Printf("  * %v\n", keyspaceName)
		for _, shard := range shards {
			shard.followProcessesLock.Lock()
			fmt.Printf("    * %v with %d followings\n", shard.String(), len(shard.followProcesses))
			for k, _ := range shard.followProcesses {
				fmt.Printf("        ~ %d.%d\n", k.ServerId, k.ShardId)
			}
			shard.followProcessesLock.Unlock()
		}
	}
	ss.keyspaceShards.RUnlock()

	fmt.Printf("\ncluster listener event processors:\n")
	ss.clusterListener.Debug("  ")

	fmt.Println()

}
