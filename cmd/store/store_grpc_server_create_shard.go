package store

import (
	"github.com/chrislusf/vasto/pb"
	"golang.org/x/net/context"
	"log"
	"fmt"
	"os"
	"github.com/chrislusf/vasto/topology"
)

// CreateShard
// 1. if the shard is already created, do nothing
func (ss *storeServer) CreateShard(ctx context.Context, request *pb.CreateShardRequest) (*pb.CreateShardResponse, error) {

	log.Printf("create shard %v", request)
	err := ss.createShards(request.Keyspace, int(request.ServerId), int(request.ClusterSize), int(request.ReplicationFactor), false, func(shardId int) *topology.BootstrapPlan {
		return &topology.BootstrapPlan{}
	})
	if err != nil {
		log.Printf("create keyspace %s: %v", request.Keyspace, err)
		return &pb.CreateShardResponse{
			Error: err.Error(),
		}, nil
	}

	return &pb.CreateShardResponse{
		Error: "",
	}, nil

}

func (ss *storeServer) createShards(keyspace string, serverId int, clusterSize, replicationFactor int, isCandidate bool, planGen func(shardId int) *topology.BootstrapPlan) (error) {

	var existingPrimaryShards []*pb.ClusterNode
	if cluster, found := ss.clusterListener.GetCluster(keyspace); found {
		for i := 0; i < cluster.ExpectedSize(); i++ {
			if n, _, ok := cluster.GetNode(i); ok {
				existingPrimaryShards = append(existingPrimaryShards, n)
			} else {
				log.Printf("missing server %d", i)
			}
		}
		log.Printf("existing shards: %+v", existingPrimaryShards)
	}

	ss.clusterListener.AddNewKeyspace(keyspace, clusterSize, replicationFactor)

	if _, found := ss.keyspaceShards.getShards(keyspace); found {
		localShards, foundLocalShards := ss.getServerStatusInCluster(keyspace)
		if !foundLocalShards {
			return fmt.Errorf("missing local shard status for keyspace %s", keyspace)
		}
		if int(localShards.ClusterSize) == clusterSize && int(localShards.ReplicationFactor) == replicationFactor {
			return fmt.Errorf("keyspace %s already exists", keyspace)
		}
		if serverId != int(localShards.Id) {
			return fmt.Errorf("local server id = %d, not matching requested server id %d", localShards.Id, serverId)
		}
	}

	localShards := ss.getOrCreateServerStatusInCluster(keyspace, serverId, clusterSize, replicationFactor)

	for _, clusterShard := range topology.LocalShards(serverId, clusterSize, replicationFactor) {

		shardInfo, foundShardInfo := localShards.ShardMap[uint32(clusterShard.ShardId)]

		if !foundShardInfo {
			shardInfo = &pb.ShardInfo{
				ServerId:          uint32(serverId),
				ShardId:           uint32(clusterShard.ShardId),
				KeyspaceName:      keyspace,
				ClusterSize:       uint32(clusterSize),
				ReplicationFactor: uint32(replicationFactor),
				IsCandidate:       isCandidate,
			}
		}

		shard, foundShard := ss.keyspaceShards.getShard(keyspace, shard_id(clusterShard.ShardId))
		if !foundShard {
			log.Printf("creating new shard %s", shardInfo.IdentifierOnThisServer())
			var shardCreationError error
			if shard, shardCreationError = ss.openShard(shardInfo); shardCreationError != nil {
				return fmt.Errorf("creating %s: %v", shardInfo.IdentifierOnThisServer(), shardCreationError)
			}
			log.Printf("created new shard %s", shard.String())
		} else {
			log.Printf("found existing shard %s", shard.String())
		}

		plan := planGen(clusterShard.ShardId)
		log.Printf("shard %s bootstrap plan: %s", shardInfo.IdentifierOnThisServer(), plan.String())

		if err := shard.startWithBootstrapPlan(plan, ss.selfAdminAddress(), existingPrimaryShards); err != nil {
			return fmt.Errorf("bootstrap shard %v : %v", shardInfo.IdentifierOnThisServer(), err)
		}

		localShards.ShardMap[uint32(clusterShard.ShardId)] = shardInfo

		if !foundShardInfo {
			ss.sendShardInfoToMaster(shardInfo, pb.ShardInfo_READY)
		}

	}

	return ss.saveClusterConfig(localShards, keyspace)

}

func (ss *storeServer) startExistingNodes(keyspaceName string, storeStatus *pb.LocalShardsInCluster) error {
	for _, shardInfo := range storeStatus.ShardMap {
		if shard, shardOpenError := ss.openShard(shardInfo); shardOpenError != nil {
			return fmt.Errorf("open %s: %v", shardInfo.IdentifierOnThisServer(), shardOpenError)
		} else {

			for fileId, meta := range shard.db.GetLiveFilesMetaData() {
				log.Printf("%d name:%s, level:%d size:%d SmallestKey:%s LargestKey:%s", fileId, meta.Name, meta.Level, meta.Size, string(meta.SmallestKey), string(meta.LargestKey))
				if meta.Level >= 6 {
					shard.hasBackfilled = true
				}
			}

			if err := shard.startWithBootstrapPlan(&topology.BootstrapPlan{
				IsNormalStart:                true,
				IsNormalStartBootstrapNeeded: *ss.option.Bootstrap,
			}, ss.selfAdminAddress(), nil); err != nil {
				return fmt.Errorf("bootstrap shard %v : %v", shardInfo.IdentifierOnThisServer(), err)
			}
		}
	}
	return nil
}

func (ss *storeServer) openShard(shardInfo *pb.ShardInfo) (shard *shard, err error) {

	cluster := ss.clusterListener.GetOrSetCluster(shardInfo.KeyspaceName, int(shardInfo.ClusterSize), int(shardInfo.ReplicationFactor))

	dir := fmt.Sprintf("%s/%s/%d", *ss.option.Dir, shardInfo.KeyspaceName, shardInfo.ShardId)
	err = os.MkdirAll(dir, 0755)
	if err != nil {
		log.Printf("mkdir %s: %v", dir, err)
		return nil, fmt.Errorf("mkdir %s: %v", dir, err)
	}

	shard = newShard(shardInfo.KeyspaceName, dir, int(shardInfo.ServerId), int(shardInfo.ShardId), cluster, ss.clusterListener,
		int(shardInfo.ReplicationFactor), *ss.option.LogFileSizeMb, *ss.option.LogFileCount)
	shard.setCompactionFilterClusterSize(int(shardInfo.ClusterSize))
	// println("loading shard", shard.String())
	ss.keyspaceShards.addShards(shardInfo.KeyspaceName, shard)
	ss.RegisterPeriodicTask(shard)
	return shard, nil

}
