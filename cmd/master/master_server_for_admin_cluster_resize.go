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

	if cluster.GetNextClusterRing() != nil {
		resp.Error = fmt.Sprintf("cluster %s %s is resizing %d => %d in progress ...",
			req.Keyspace, req.DataCenter, cluster.CurrentSize(), cluster.GetNextClusterRing().ExpectedSize())
		return
	}

	if cluster.ExpectedSize() == int(req.GetTargetClusterSize()) {
		resp.Error = fmt.Sprintf("cluster %s %s is already size %d", req.Keyspace, req.DataCenter, cluster.ExpectedSize())
		return
	}

	var existingServers, newServers []*pb.StoreResource
	for _, node := range cluster.GetNodes() {
		existingServers = append(existingServers, &pb.StoreResource{
			Address:      node.GetAddress(),
			AdminAddress: node.GetAdminAddress(),
		})
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
func allocateServers(cluster *topology.ClusterRing, dc *dataCenter, serverCount int, eachShardSizeGb float64) ([]*pb.StoreResource, error) {
	servers, err := dc.allocateServers(serverCount, eachShardSizeGb,
		func(resource *pb.StoreResource) bool {

			for _, node := range cluster.GetNodes() {
				if node.GetAddress() == resource.GetAddress() {
					return false
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

func (ms *masterServer) adjustAndBroadcastUpcomingShardStatuses(ctx context.Context, req *pb.ResizeRequest, cluster *topology.ClusterRing, newStores []*pb.StoreResource, existingServers []*pb.StoreResource) error {

	log.Printf("adjustAndBroadcastUpcomingShardStatuses %v", req)

	// wait a little bit for shards created and update back shard status to master
	time.Sleep(time.Second)
	// TODO wait until all updated shards are reported back

	candidateCluster := cluster.GetNextClusterRing()

	newClusterSize := int(req.TargetClusterSize)
	oldClusterSize := cluster.ExpectedSize()
	replicationFactor := cluster.ReplicationFactor()

	maxClusterSize := newClusterSize
	if maxClusterSize < oldClusterSize {
		maxClusterSize = oldClusterSize
	}

	// report new shards, update shards, and delete retiring shards
	for serverId := 0; serverId < maxClusterSize; serverId++ {

		oldServer, _, oldServerFound := cluster.GetNode(serverId)
		if oldServerFound {
			// these should be to-be-removed servers, or no-change servers, only change the cluster size
			for _, shardInfo := range oldServer.GetShardInfoList() {
				if topology.IsShardInLocal(int(shardInfo.ShardId), int(shardInfo.ServerId), newClusterSize, replicationFactor) {
					// change shards that will be on the new cluster
					shardInfo.IsCandidate = false
					shardInfo.ClusterSize = uint32(newClusterSize)
					ms.notifyUpdate(shardInfo, oldServer.GetStoreResource())
					log.Printf("change shard %v on %s to cluster size %d", shardInfo.IdentifierOnThisServer(), oldServer.GetAddress(), newClusterSize)
				} else {
					oldServer.RemoveShardInfo(shardInfo)
					shardInfo.IsPermanentDelete = true
					ms.notifyDeletion(shardInfo, oldServer.GetStoreResource())
					log.Printf("delete shard %v on %s for cluster size %d", shardInfo.IdentifierOnThisServer(), oldServer.GetAddress(), newClusterSize)
				}
			}
			if len(oldServer.GetShardInfoList()) == 0 {
				cluster.RemoveNode(serverId)
			}
		}

		if candidateCluster == nil {
			continue // no new servers or shards are created
		}

		if newServer, _, newServerFound := candidateCluster.GetNode(serverId); newServerFound {
			// these are new servers with new or updated shards
			for _, shardInfo := range newServer.GetShardInfoList() {
				if topology.IsShardInLocal(int(shardInfo.ShardId), int(shardInfo.ServerId), newClusterSize, replicationFactor) {
					// double check shards that will be on the new cluster, may not be necessary
					shardInfo.IsCandidate = false
					ms.notifyPromotion(shardInfo, newServer.GetStoreResource())
					log.Printf("promote shard %v on %s to cluster size %d", shardInfo.IdentifierOnThisServer(), newServer.GetAddress(), newClusterSize)
				} else {
					log.Printf("something wrong here! %s on server %s", shardInfo.IdentifierOnThisServer(), newServer.GetAddress())
				}
			}

			// promote the node into the cluster
			candidateCluster.RemoveNode(serverId)
			if candidateCluster.CurrentSize() == 0 {
				cluster.RemoveNextClusterRing()
			}
			if !oldServerFound {
				// simply move the new server into the cluster
				cluster.SetNode(newServer)
			} else {
				// move off the shards onto the existing server
				for _, shardInfo := range newServer.GetShardInfoList() {
					oldServer.SetShardInfo(shardInfo)
				}
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
