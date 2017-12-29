package master

import (
	"fmt"
	"github.com/chrislusf/vasto/pb"
	"context"
	"strings"
	"strconv"
	"google.golang.org/grpc"
	"log"
	"github.com/chrislusf/vasto/topology"
)

func (ms *masterServer) ReplaceNode(ctx context.Context, req *pb.ReplaceNodeRequest) (resp *pb.ReplaceNodeResponse, err error) {

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

	oldServer, _, found := cluster.GetNode(int(req.NodeId))
	if !found {
		resp.Error = fmt.Sprintf("no server %v found", req.NodeId)
		return
	}

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
		log.Printf("replicateNodePrepare %v: %v", req, err)
		resp.Error = err.Error()
		return
	}

	if err = replicateNodeCommit(ctx, req, cluster, newStore, oldServer); err != nil {
		log.Printf("replicateNodeCommit %v: %v", req, err)
		resp.Error = err.Error()
		return
	}

	if err = ms.adjustAndBroadcastShardStatus(ctx, req, cluster, newStore, oldServer); err != nil {
		log.Printf("adjustAndBroadcastShardStatus %v: %v", req, err)
		resp.Error = err.Error()
		return
	}

	if err = replicateNodeCleanup(ctx, req, cluster, newStore, oldServer); err != nil {
		log.Printf("replicateNodeCleanup %v: %v", req, err)
		resp.Error = err.Error()
		return
	}

	return resp, nil

}

// 1. create the new shard and follow the old shard and its peers
func replicateNodePrepare(ctx context.Context, req *pb.ReplaceNodeRequest, cluster *topology.ClusterRing, newStore *pb.StoreResource, oldServer topology.Node) error {

	log.Printf("replicateNodePrepare %v", req)

	return withConnection(newStore, func(grpcConnection *grpc.ClientConn) error {

		client := pb.NewVastoStoreClient(grpcConnection)
		request := &pb.ReplicateNodePrepareRequest{
			Keyspace:          req.Keyspace,
			ServerId:          req.NodeId,
			ClusterSize:       uint32(cluster.ExpectedSize()),
			ReplicationFactor: uint32(cluster.ReplicationFactor()),
		}

		log.Printf("prepare replicate keyspace %s from %s to %v: %v", req.Keyspace, oldServer.GetAddress(), newStore.Address, request)
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
func replicateNodeCommit(ctx context.Context, req *pb.ReplaceNodeRequest, cluster *topology.ClusterRing, newStore *pb.StoreResource, oldServer topology.Node) error {

	log.Printf("replicateNodeCommit %v", req)

	return withConnection(newStore, func(grpcConnection *grpc.ClientConn) error {

		request := &pb.ReplicateNodeCommitRequest{
			Keyspace: req.Keyspace,
		}

		log.Printf("commit replicate keyspace %s from %s to %v: %v", req.Keyspace, oldServer.GetAddress(), newStore.Address, request)
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
func (ms *masterServer) adjustAndBroadcastShardStatus(ctx context.Context, req *pb.ReplaceNodeRequest, cluster *topology.ClusterRing, newStore *pb.StoreResource, oldServer topology.Node) error {

	log.Printf("adjustAndBroadcastShardStatus %v", req)

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

// 4. let the server to remove the old shard
func replicateNodeCleanup(ctx context.Context, req *pb.ReplaceNodeRequest, cluster *topology.ClusterRing, newStore *pb.StoreResource, oldServer topology.Node) error {

	log.Printf("replicateNodeCleanup %v", req)

	return withConnection(oldServer.GetStoreResource(), func(grpcConnection *grpc.ClientConn) error {

		request := &pb.ReplicateNodeCleanupRequest{
			Keyspace: req.Keyspace,
		}

		log.Printf("replicateNodeCleanup keyspace %s from %s to %v: %v", req.Keyspace, oldServer.GetAddress(), newStore.Address, request)
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
