package master

import (
	"fmt"
	"github.com/chrislusf/vasto/pb"
	"context"
	"github.com/chrislusf/vasto/topology"
	"google.golang.org/grpc"
	"log"
	"time"
)

func (ms *masterServer) ResizeCluster(ctx context.Context, req *pb.ResizeRequest) (resp *pb.ResizeResponse, err error) {

	resp = &pb.ResizeResponse{}

	keyspace, found := ms.topo.keyspaces.getKeyspace(req.Keyspace)
	if !found {
		resp.Error = fmt.Sprintf("no keyspace %v found", req.Keyspace)
		return
	}

	cluster, found := keyspace.getCluster(req.DataCenter)
	if !found {
		resp.Error = fmt.Sprintf("no datacenter %v found", req.DataCenter)
		return
	}

	dc, found := ms.topo.dataCenters.getDataCenter(req.DataCenter)
	if !found {
		resp.Error = fmt.Sprintf("no datacenter %v found", req.DataCenter)
		return
	}

	if cluster.GetNextCluster() != nil && cluster.GetNextCluster().CurrentSize() > 0 {
		resp.Error = fmt.Sprintf("cluster %s %s is resizing %d => %d in progress ...",
			req.Keyspace, req.DataCenter, cluster.CurrentSize(), cluster.GetNextCluster().ExpectedSize())
		return
	}

	if cluster.ExpectedSize() == int(req.GetTargetClusterSize()) {
		resp.Error = fmt.Sprintf("cluster %s %s is already size %d", req.Keyspace, req.DataCenter, cluster.ExpectedSize())
		return
	}

	var existingServers, newServers []*pb.StoreResource
	for i := 0; i < cluster.ExpectedSize(); i++ {
		if node, _, found := cluster.GetNode(i); found {
			existingServers = append(existingServers, node.StoreResource)
		}
	}

	// 1. allocate new servers for the growing cluster
	if cluster.ExpectedSize() < int(req.GetTargetClusterSize()) {
		// grow the cluster
		var allocateErr error
		// TODO proper quota alocation
		eachShardSizeGb := uint32(1)
		newServers, allocateErr = allocateServers(cluster, dc, int(req.TargetClusterSize)-cluster.ExpectedSize(), float64(eachShardSizeGb))
		if allocateErr != nil {
			log.Printf("allocateServers %v: %v", req, err)
			resp.Error = fmt.Sprintf("fail to allocate %d servers: %v", int(req.TargetClusterSize)-cluster.ExpectedSize(), allocateErr)
			return
		}

	} else {
		// shrink the cluster
	}

	// 2. create missing shards on existing servers, create new shards on new servers
	servers := append(existingServers, newServers...)
	if err = resizeCreateShards(ctx, req.Keyspace, uint32(cluster.ExpectedSize()), req.TargetClusterSize, uint32(cluster.ReplicationFactor()), servers); err != nil {
		log.Printf("resizeCreateShards %v: %v", req, err)
		resp.Error = err.Error()
		return
	}

	// 3. tell all servers to commit the new shards, adjust local cluster size, status, etc, not informing the master of shard info changes
	if err = resizeCommit(ctx, req.Keyspace, req.TargetClusterSize, servers); err != nil {
		resp.Error = err.Error()
		return
	}

	if err = ms.adjustAndBroadcastUpcomingShardStatuses(ctx, req, cluster, servers, existingServers); err != nil {
		log.Printf("adjustAndBroadcastUpcomingShardStatuses %v: %v", req, err)
		resp.Error = err.Error()
		return
	}

	// 3. cleanup old shards
	if err = resizeCleanup(ctx, req.Keyspace, req.TargetClusterSize, servers); err != nil {
		log.Printf("resizeCleanup %v: %v", req, err)
		resp.Error = err.Error()
		return
	}

	cluster.SetExpectedSize(int(req.TargetClusterSize))

	return
}

// TODO add tags for filtering
func allocateServers(cluster *topology.Cluster, dc *dataCenter, serverCount int, eachShardSizeGb float64) ([]*pb.StoreResource, error) {
	servers, err := dc.allocateServers(serverCount, eachShardSizeGb,
		func(resource *pb.StoreResource) bool {

			for i := 0; i < cluster.ExpectedSize(); i++ {
				if node, _, found := cluster.GetNode(i); found {
					if node.StoreResource.GetAddress() == resource.GetAddress() {
						return false
					}
				}
			}

			return true
		})

	return servers, err
}

func resizeCreateShards(ctx context.Context, keyspace string, clusterSize, targetClusterSize, replicationFactor uint32, stores []*pb.StoreResource) (error) {

	return eachStore(stores, func(serverId int, store *pb.StoreResource) error {
		// log.Printf("connecting to server %d at %s", serverId, store.GetAdminAddress())
		return withConnection(store, func(grpcConnection *grpc.ClientConn) error {

			client := pb.NewVastoStoreClient(grpcConnection)
			request := &pb.ResizeCreateShardRequest{
				Keyspace:          keyspace,
				ServerId:          uint32(serverId),
				ClusterSize:       clusterSize,
				ReplicationFactor: replicationFactor,
				TargetClusterSize: targetClusterSize,
			}

			log.Printf("resize create shard on %v: %v", store.AdminAddress, request)
			resp, err := client.ResizePrepare(ctx, request)
			if err != nil {
				return err
			}
			if resp.Error != "" {
				return fmt.Errorf("resize create shard %d on %s: %s", serverId, store.AdminAddress, resp.Error)
			}
			return nil
		})
	})
}

func resizeCommit(ctx context.Context, keyspace string, clusterSize uint32, stores []*pb.StoreResource) (error) {

	return eachStore(stores, func(serverId int, store *pb.StoreResource) error {
		// log.Printf("connecting to server %d at %s", serverId, store.GetAdminAddress())
		return withConnection(store, func(grpcConnection *grpc.ClientConn) error {

			client := pb.NewVastoStoreClient(grpcConnection)
			request := &pb.ResizeCommitRequest{
				Keyspace:          keyspace,
				TargetClusterSize: clusterSize,
			}

			log.Printf("resize commit on %v: %v", store.AdminAddress, request)
			resp, err := client.ResizeCommit(ctx, request)
			if err != nil {
				return err
			}
			if resp.Error != "" {
				return fmt.Errorf("resize commit server %d on %s: %s", serverId, store.AdminAddress, resp.Error)
			}
			return nil
		})
	})
}

func (ms *masterServer) adjustAndBroadcastUpcomingShardStatuses(ctx context.Context, req *pb.ResizeRequest, cluster *topology.Cluster, newStores []*pb.StoreResource, existingServers []*pb.StoreResource) error {

	log.Printf("adjustAndBroadcastUpcomingShardStatuses %v", req)

	// wait a little bit for shards created and update back shard status to master
	time.Sleep(time.Second)

	candidateCluster := cluster.GetNextCluster()

	oldClusterSize := cluster.ExpectedSize()
	newClusterSize := int(req.TargetClusterSize)
	replicationFactor := cluster.ReplicationFactor()

	// promote candidate shards into the real cluster
	if candidateCluster != nil {
		for _, logicalShardGroup := range candidateCluster.GetAllShards() {
			for _, node := range logicalShardGroup {
				node.ShardInfo.IsCandidate = false
				cluster.SetShard(node.StoreResource, node.ShardInfo)
				ms.notifyPromotion(node.ShardInfo, node.StoreResource)
				log.Printf("promoting new shard %v on %s", node.ShardInfo.IdentifierOnThisServer(), node.StoreResource.GetAddress())
			}
		}
	}
	cluster.RemoveNextCluster()

	// fix existing shards and drop retiring shards
	var toBeRemoved []*pb.ClusterNode
	for _, logicalShardGroup := range cluster.GetAllShards() {
		for _, node := range logicalShardGroup {
			if topology.IsShardInLocal(int(node.ShardInfo.ShardId), int(node.ShardInfo.ServerId), newClusterSize, replicationFactor) {
				if int(node.ShardInfo.ClusterSize) != newClusterSize {
					node.ShardInfo.ClusterSize = uint32(newClusterSize)
					ms.notifyUpdate(node.ShardInfo, node.GetStoreResource())
					log.Printf("change shard %v on %s to cluster size %d", node.ShardInfo.IdentifierOnThisServer(), node.StoreResource.GetAddress(), newClusterSize)
				}
			} else {
				// move removing outside to avoid modifying when iterating
				toBeRemoved = append(toBeRemoved, node)
			}
		}
	}

	// notify the new cluster size, clients can write to the new set of servers now
	ms.clientChans.notifyClusterResize(keyspace_name(req.Keyspace), data_center_name(req.DataCenter), uint32(oldClusterSize), req.TargetClusterSize)

	// wait a bit for the slow-to-change clients
	time.Sleep(5 * time.Second)

	// remove retiring shards
	for _, node := range toBeRemoved {
		cluster.RemoveShard(node.StoreResource, node.ShardInfo)
		node.ShardInfo.IsPermanentDelete = true
		ms.notifyDeletion(node.ShardInfo, node.GetStoreResource())
		log.Printf("delete shard %v on %s for cluster size %d", node.ShardInfo.IdentifierOnThisServer(), node.StoreResource.GetAddress(), newClusterSize)
	}

	// notify clients of shards on to-be-cleanup servers, if shrinking
	for i := newClusterSize; i < oldClusterSize; i++ {
		if node, _, found := cluster.GetNode(i); found {
			store := node.StoreResource
			for _, shardInfo := range cluster.RemoveStore(store) {
				shardInfo.IsPermanentDelete = true
				ms.notifyDeletion(shardInfo, store)
				log.Printf("remove shard %v on %s for cluster size %d", shardInfo.IdentifierOnThisServer(), store.GetAddress(), newClusterSize)
			}
		}
	}

	return nil
}

func resizeCleanup(ctx context.Context, keyspace string, clusterSize uint32, stores []*pb.StoreResource) (error) {

	return eachStore(stores, func(serverId int, store *pb.StoreResource) error {
		// log.Printf("connecting to server %d at %s", serverId, store.GetAdminAddress())
		return withConnection(store, func(grpcConnection *grpc.ClientConn) error {

			client := pb.NewVastoStoreClient(grpcConnection)
			request := &pb.ResizeCleanupRequest{
				Keyspace:          keyspace,
				TargetClusterSize: clusterSize,
			}

			log.Printf("resize cleanup on %v: %v", store.AdminAddress, request)
			resp, err := client.ResizeCleanup(ctx, request)
			if err != nil {
				return err
			}
			if resp.Error != "" {
				return fmt.Errorf("resize cleanup server %d on %s: %s", serverId, store.AdminAddress, resp.Error)
			}
			return nil
		})
	})
}
