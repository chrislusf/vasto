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

	if cluster.ExpectedSize() == int(req.GetClusterSize()) {
		resp.Error = fmt.Sprintf("cluster %s %s is already size %d", req.Keyspace, req.DataCenter, cluster.ExpectedSize())
		return
	}

	var existingServers []*pb.StoreResource
	for _, node := range cluster.GetNodes() {
		existingServers = append(existingServers, &pb.StoreResource{
			Address:      node.GetAddress(),
			AdminAddress: node.GetAdminAddress(),
		})
	}

	if cluster.ExpectedSize() < int(req.GetClusterSize()) {

		// grow the cluster
		// TODO proper quota alocation
		// 1. allocate new servers for the growing cluster
		eachShardSizeGb := uint32(1)
		newServers, allocateErr := allocateServers(cluster, dc, int(req.ClusterSize)-cluster.ExpectedSize(), float64(eachShardSizeGb))
		if allocateErr != nil {
			resp.Error = fmt.Sprintf("fail to allocate %d servers: %v", int(req.ClusterSize)-cluster.ExpectedSize(), allocateErr)
			return
		}

		// 2. create missing shards on existing servers, create new shards on new servers
		servers := append(existingServers, newServers...)
		if err = resizeCreateShards(ctx, req.Keyspace, req.ClusterSize, uint32(cluster.ReplicationFactor()), servers); err != nil {
			resp.Error = err.Error()
			return
		}

		// 3. tell all servers to commit the new shards, adjust local cluster size, status, etc, not informing the master of shard info changes
		if err = resizeCommit(ctx, req.Keyspace, req.ClusterSize, servers); err != nil {
			resp.Error = err.Error()
			return
		}

		if err = ms.adjustAndBroadcastUpcomingShardStatuses(ctx, req, cluster, servers, existingServers); err != nil {
			log.Printf("adjustAndBroadcastUpcomingShardStatuses %v: %v", req, err)
			resp.Error = err.Error()
			return
		}

	} else {
		// shrink the cluster
	}

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

func resizeCreateShards(ctx context.Context, keyspace string, clusterSize, replicationFactor uint32, stores []*pb.StoreResource) (error) {

	return eachStore(stores, func(serverId int, store *pb.StoreResource) error {
		// log.Printf("connecting to server %d at %s", serverId, store.GetAdminAddress())
		return withConnection(store, func(grpcConnection *grpc.ClientConn) error {

			client := pb.NewVastoStoreClient(grpcConnection)
			request := &pb.ResizeCreateShardRequest{
				Keyspace:          keyspace,
				ServerId:          uint32(serverId),
				ClusterSize:       clusterSize,
				ReplicationFactor: replicationFactor,
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

			log.Printf("resize create shard on %v: %v", store.AdminAddress, request)
			resp, err := client.ResizeCommit(ctx, request)
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

func (ms *masterServer) adjustAndBroadcastUpcomingShardStatuses(ctx context.Context, req *pb.ResizeRequest, cluster *topology.ClusterRing, newStores []*pb.StoreResource, existingServers []*pb.StoreResource) error {

	log.Printf("adjustAndBroadcastUpcomingShardStatuses %v", req)

	// wait a little bit for shards created and update back shard status to master
	time.Sleep(time.Second)
	// TODO wait until all updated shards are reported back

	candidateCluster := cluster.GetNextClusterRing()
	if candidateCluster == nil {
		return fmt.Errorf("candidate cluster for keyspace %s does not exist", req.Keyspace)
	}

	for i := 0; i < cluster.ExpectedSize(); i++ {
		n, _, found := cluster.GetNode(i)
		if !found {
			continue
		}
		if n.GetAdminAddress() != oldServer.GetAdminAddress() {
			continue
		}

		candidate, _, found := candidateCluster.GetNode(i)
		if !found {
			return fmt.Errorf("candidate server for keyspace %s server %s does not exist", req.Keyspace, n.GetAddress())
		}

		// remove the old shard
		cluster.RemoveNode(n.GetId())
		for _, shardInfo := range n.GetShardInfoList() {
			shardInfo.IsPermanentDelete = true
			ms.notifyDeletion(shardInfo, n.GetStoreResource())
			log.Printf("removing old shard %v on %s", shardInfo.IdentifierOnThisServer(), n.GetAddress())
		}

		// promote the new shard
		candidateCluster.RemoveNode(i)
		if candidateCluster.CurrentSize() == 0 {
			cluster.RemoveNextClusterRing()
		}
		cluster.SetNode(candidate)
		for _, shardInfo := range candidate.GetShardInfoList() {
			shardInfo.IsCandidate = false
			ms.notifyPromotion(shardInfo, candidate.GetStoreResource())
			log.Printf("promoting new shard %v on %s", shardInfo.IdentifierOnThisServer(), candidate.GetAddress())
		}

	}

	return nil
}
