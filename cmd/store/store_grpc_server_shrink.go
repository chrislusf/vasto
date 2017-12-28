package store

import (
	"github.com/chrislusf/vasto/pb"
	"golang.org/x/net/context"
	"log"
	"fmt"
	"sync"
	"github.com/chrislusf/vasto/topology"
	"google.golang.org/grpc"
)

func (ss *storeServer) localShardsAfterShrink(keyspace string, targetClusterSize uint32) (shards []*shard, found bool) {
	localShards, found := ss.keyspaceShards.getShards(keyspace)
	if !found {
		return
	}
	for _, shard := range localShards {
		s := shard
		if uint32(s.id) > targetClusterSize {
			continue
		}
		shards = append(shards, s)
	}
	return
}

// 1. master tell shard As the new cluster size
func (ss *storeServer) ShrinkClusterPrepare(ctx context.Context, request *pb.ShrinkClusterPrepareRequest) (*pb.ShrinkClusterPrepareResponse, error) {

	log.Printf("shrink cluster prepare %v", request)
	err := ss.copyFromRetiringShards(ctx, request)
	if err != nil {
		log.Printf("shrink cluster prepare %v: %v", request, err)
		return &pb.ShrinkClusterPrepareResponse{
			Error: err.Error(),
		}, nil
	}

	go ss.followRetiringShards(request)

	return &pb.ShrinkClusterPrepareResponse{
		Error: "",
	}, nil

}

// 2. let the server to commit the changed cluster size to disk
func (ss *storeServer) ShrinkClusterCommit(ctx context.Context, request *pb.ShrinkClusterCommitRequest) (*pb.ShrinkClusterCommitResponse, error) {

	log.Printf("shrink cluster %v", request)
	err := ss.commitClusterSizeChange(request)
	if err != nil {
		log.Printf("shrink cluster %v: %v", request, err)
		return &pb.ShrinkClusterCommitResponse{
			Error: err.Error(),
		}, nil
	}
	ss.keyspaceShards.deleteKeyspace(request.Keyspace)

	return &pb.ShrinkClusterCommitResponse{
		Error: "",
	}, nil

}

// 3. let the server to remove the old shard
func (ss *storeServer) ShrinkClusterCleanup(ctx context.Context, request *pb.ShrinkClusterCleanupRequest) (*pb.ShrinkClusterCleanupResponse, error) {

	log.Printf("shrink cluster %v", request)
	err := ss.deleteShards(request.Keyspace)
	if err != nil {
		log.Printf("shrink cluster %v: %v", request, err)
		return &pb.ShrinkClusterCleanupResponse{
			Error: err.Error(),
		}, nil
	}

	return &pb.ShrinkClusterCleanupResponse{
		Error: "",
	}, nil

}

func (ss *storeServer) createMissingShards(ctx context.Context, keyspaceName string, targetClusterSize int, replicationFactor int) (err error) {

	statusInCluster, found := ss.getServerStatusInCluster(keyspaceName)
	if !found {
		return fmt.Errorf("not found keyspace %s", keyspaceName)
	}

	var missingShardIds []int
	for _, clusterShard := range topology.LocalShards(int(statusInCluster.Id), targetClusterSize, replicationFactor) {
		if _, found := statusInCluster.ShardMap[uint32(clusterShard.ShardId)]; !found {
			missingShardIds = append(missingShardIds, clusterShard.ShardId)
		}
	}

	return eachInt(missingShardIds, func(shardId int) error {

		bootstrapPlan := topology.BootstrapPlanWithTopoChange(&topology.BootstrapRequest{
			ServerId:          int(statusInCluster.Id),
			ShardId:           shardId,
			FromClusterSize:   int(statusInCluster.ClusterSize),
			ToClusterSize:     targetClusterSize,
			ReplicationFactor: replicationFactor,
		})
		shardInfo := &pb.ShardInfo{
			NodeId:            statusInCluster.Id,
			ShardId:           uint32(shardId),
			KeyspaceName:      keyspaceName,
			ClusterSize:       uint32(targetClusterSize),
			ReplicationFactor: uint32(replicationFactor),
			IsCandidate:       true,
		}
		localShards := ss.getOrCreateServerStatusInCluster(keyspaceName, int(statusInCluster.Id), targetClusterSize, replicationFactor)

		ss.bootstrapShard(shardInfo, bootstrapPlan)

		localShards.ShardMap[uint32(shardId)] = shardInfo

		ss.sendShardInfoToMaster(shardInfo, pb.ShardInfo_READY)

		ss.saveClusterConfig(localShards, keyspaceName)
		return nil
	})

}

func (ss *storeServer) copyFromRetiringShards(ctx context.Context, request *pb.ShrinkClusterPrepareRequest) (err error) {

	localShards, found := ss.localShardsAfterShrink(request.Keyspace, request.TargetClusterSize)
	if !found {
		return fmt.Errorf("not found keyspace %s", request.Keyspace)
	}

	return eachShard(localShards, func(shard *shard) error {
		var retiringServerIds []int
		for i := shard.clusterRing.ExpectedSize(); i < int(request.TargetClusterSize); i++ {
			retiringServerIds = append(retiringServerIds, i)
		}

		return eachInt(retiringServerIds, func(serverId int) error {
			return shard.clusterRing.WithConnection(serverId, func(node topology.Node, grpcConnection *grpc.ClientConn) error {
				_, _, copyErr := shard.writeToSst(ctx, grpcConnection, request.TargetClusterSize, int(shard.id))
				return copyErr
			})
		})
	})

}

func (ss *storeServer) followRetiringShards(request *pb.ShrinkClusterPrepareRequest) (err error) {

	localShards, found := ss.localShardsAfterShrink(request.Keyspace, request.TargetClusterSize)
	if !found {
		return fmt.Errorf("not found keyspace %s", request.Keyspace)
	}

	return eachShard(localShards, func(shard *shard) error {
		var retiringServerIds []int
		for i := shard.clusterRing.ExpectedSize(); i < int(request.TargetClusterSize); i++ {
			retiringServerIds = append(retiringServerIds, i)
		}

		return eachInt(retiringServerIds, func(serverId int) error {
			return shard.clusterRing.WithConnection(serverId, func(node topology.Node, grpcConnection *grpc.ClientConn) error {
				return shard.followChanges(context.Background(), node, grpcConnection, request.TargetClusterSize, int(shard.id))
			})
		})
	})

}

func (ss *storeServer) commitClusterSizeChange(request *pb.ShrinkClusterCommitRequest) (err error) {

	localShardsStatus, found := ss.getServerStatusInCluster(request.Keyspace)
	if !found {
		return fmt.Errorf("not found keyspace %s", request.Keyspace)
	}

	for _, shardInfo := range localShardsStatus.ShardMap {
		shardInfo.ClusterSize = request.TargetClusterSize
	}

	ss.saveClusterConfig(localShardsStatus, request.Keyspace)

	return nil
}

func eachShard(shards []*shard, eachFunc func(*shard) error) (err error) {
	var wg sync.WaitGroup
	for _, s := range shards {
		wg.Add(1)
		go func(s *shard) {
			defer wg.Done()
			if eachErr := eachFunc(s); eachErr != nil {
				err = eachErr
			}
		}(s)
	}
	wg.Wait()
	return
}

func eachInt(ints []int, eachFunc func(x int) error) (err error) {
	var wg sync.WaitGroup
	for _, x := range ints {
		wg.Add(1)
		go func(x int) {
			defer wg.Done()
			if eachErr := eachFunc(x); eachErr != nil {
				err = eachErr
			}
		}(x)
	}
	wg.Wait()
	return
}
