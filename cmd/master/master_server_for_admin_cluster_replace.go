package master

import (
	"context"
	"fmt"
	"github.com/chrislusf/glog"
	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/topology"
	"google.golang.org/grpc"
	"strconv"
	"strings"
	"time"
)

func (ms *masterServer) ReplaceNode(ctx context.Context, req *pb.ReplaceNodeRequest) (resp *pb.ReplaceNodeResponse, err error) {

	ms.lock(req.Keyspace)
	defer ms.unlock(req.Keyspace)

	resp = &pb.ReplaceNodeResponse{}

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

	if cluster.GetNextCluster() != nil && cluster.GetNextCluster().CurrentSize() > 0 {
		resp.Error = fmt.Sprintf("cluster %s %s is changing %d => %d in progress ...",
			req.Keyspace, req.DataCenter, cluster.ExpectedSize(), cluster.GetNextCluster().ExpectedSize())
		return
	}

	oldServerNode, _, found := cluster.GetNode(int(req.NodeId))
	if !found {
		resp.Error = fmt.Sprintf("no server %v found", req.NodeId)
		return
	}
	oldServer := oldServerNode.StoreResource

	adminAddress, err := addressToAdminAddress(req.NewAddress)
	if err != nil {
		resp.Error = err.Error()
		return
	}

	newStore := &pb.StoreResource{
		Address:      req.GetNewAddress(),
		AdminAddress: adminAddress,
	}

	if err = replicateNodePrepare(ctx, req, cluster, newStore, oldServer); err != nil {
		glog.Errorf("replicateNodePrepare %v: %v", req, err)
		resp.Error = err.Error()
		return
	}

	if err = replicateNodeCommit(ctx, req, cluster, newStore, oldServer); err != nil {
		glog.Errorf("replicateNodeCommit %v: %v", req, err)
		resp.Error = err.Error()
		return
	}

	if err = ms.adjustAndBroadcastShardStatus(ctx, req, cluster, newStore, oldServer); err != nil {
		glog.Errorf("adjustAndBroadcastShardStatus %v: %v", req, err)
		resp.Error = err.Error()
		return
	}

	if err = replicateNodeCleanup(ctx, req, cluster, newStore, oldServer); err != nil {
		glog.Errorf("replicateNodeCleanup %v: %v", req, err)
		resp.Error = err.Error()
		return
	}

	return resp, nil

}

// 1. create the new shard and follow the old shard and its peers
func replicateNodePrepare(ctx context.Context, req *pb.ReplaceNodeRequest, cluster *topology.Cluster, newStore *pb.StoreResource, oldServer *pb.StoreResource) error {

	glog.V(1).Infof("replicateNodePrepare %v", req)

	return withConnection(newStore, func(grpcConnection *grpc.ClientConn) error {

		client := pb.NewVastoStoreClient(grpcConnection)
		request := &pb.ReplicateNodePrepareRequest{
			Keyspace:          req.Keyspace,
			ServerId:          req.NodeId,
			ClusterSize:       uint32(cluster.ExpectedSize()),
			ReplicationFactor: uint32(cluster.ReplicationFactor()),
		}

		glog.V(1).Infof("prepare replicate keyspace %s from %s to %v: %v", req.Keyspace, oldServer.GetAddress(), newStore.Address, request)
		resp, err := client.ReplicateNodePrepare(ctx, request)
		if err != nil {
			return err
		}
		if resp.Error != "" {
			return fmt.Errorf("prepare replicate keyspace %s from %s to %v: %s", req.Keyspace, oldServer.GetAddress(), newStore.Address, resp.Error)
		}
		return nil
	})
}

// 2. let the server to promote the new shard from CANDIDATE to READY
func replicateNodeCommit(ctx context.Context, req *pb.ReplaceNodeRequest, cluster *topology.Cluster, newStore *pb.StoreResource, oldServer *pb.StoreResource) error {

	glog.V(1).Infof("replicateNodeCommit %v", req)

	return withConnection(newStore, func(grpcConnection *grpc.ClientConn) error {

		request := &pb.ReplicateNodeCommitRequest{
			Keyspace: req.Keyspace,
		}

		glog.V(1).Infof("commit replicate keyspace %s from %s to %v: %v", req.Keyspace, oldServer.GetAddress(), newStore.Address, request)
		resp, err := pb.NewVastoStoreClient(grpcConnection).ReplicateNodeCommit(ctx, request)
		if err != nil {
			return err
		}
		if resp.Error != "" {
			return fmt.Errorf("commit replicate keyspace %s from %s to %v: %s", req.Keyspace, oldServer.GetAddress(), newStore.Address, resp.Error)
		}
		return nil
	})
}

// 3. remove the old shard, set the new shard from CANDIDATE to READY, and inform all clients of these changes
func (ms *masterServer) adjustAndBroadcastShardStatus(ctx context.Context, req *pb.ReplaceNodeRequest, cluster *topology.Cluster, newStore *pb.StoreResource, oldServer *pb.StoreResource) error {

	glog.V(1).Infof("adjustAndBroadcastShardStatus %v", req)

	// wait a little bit for shards created and update back shard status to master
	time.Sleep(time.Second)

	candidateCluster := cluster.GetNextCluster()
	if candidateCluster == nil {
		return fmt.Errorf("candidate cluster for keyspace %s does not exist", req.Keyspace)
	}

	for i := 0; i < cluster.ExpectedSize(); i++ {
		n, _, found := cluster.GetNode(i)
		if !found {
			continue
		}
		if n.StoreResource.GetAdminAddress() != oldServer.GetAdminAddress() {
			continue
		}

		candidate, _, found := candidateCluster.GetNode(i)
		if !found {
			return fmt.Errorf("candidate server for keyspace %s server %s does not exist", req.Keyspace, n.StoreResource.GetAddress())
		}

		// promote the new shard
		promotedShards := candidateCluster.RemoveStore(candidate.GetStoreResource())
		if candidateCluster.CurrentSize() == 0 {
			cluster.RemoveNextCluster()
		}
		for _, shardInfo := range promotedShards {
			shardInfo.IsCandidate = false
			if cluster.ReplaceShard(candidate.StoreResource, shardInfo) {
				ms.notifyPromotion(shardInfo, candidate.GetStoreResource())
				glog.V(1).Infof("promoting new shard %v on %s", shardInfo.IdentifierOnThisServer(), candidate.StoreResource.GetAddress())
			}
		}

		// wait a bit for the slow-to-change clients
		time.Sleep(5 * time.Second)

	}

	return nil
}

// 4. let the server to remove the old shard
func replicateNodeCleanup(ctx context.Context, req *pb.ReplaceNodeRequest, cluster *topology.Cluster, newStore *pb.StoreResource, oldServer *pb.StoreResource) error {

	glog.V(1).Infof("replicateNodeCleanup %v", req)

	return withConnection(oldServer, func(grpcConnection *grpc.ClientConn) error {

		request := &pb.ReplicateNodeCleanupRequest{
			Keyspace: req.Keyspace,
		}

		glog.V(1).Infof("replicateNodeCleanup keyspace %s from %s to %v: %v", req.Keyspace, oldServer.GetAddress(), newStore.Address, request)
		resp, err := pb.NewVastoStoreClient(grpcConnection).ReplicateNodeCleanup(ctx, request)
		if err != nil {
			return err
		}
		if resp.Error != "" {
			return fmt.Errorf("replicateNodeCleanup keyspace %s from %s to %v: %s", req.Keyspace, oldServer.GetAddress(), newStore.Address, resp.Error)
		}
		return nil
	})
}

func addressToAdminAddress(address string) (string, error) {
	parts := strings.SplitN(address, ":", 2)
	port, err := strconv.ParseUint(parts[1], 10, 32)
	if err != nil {
		return "", fmt.Errorf("parse address %v: %v", address, err)
	}
	port += 10000
	return fmt.Sprintf("%s:%d", parts[0], port), nil
}
