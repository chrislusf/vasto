package master

import (
	"fmt"
	"github.com/chrislusf/vasto/pb"
	"context"
	"strings"
	"strconv"
	"google.golang.org/grpc"
	"log"
)

func (ms *masterServer) ReplaceNodePrepare(ctx context.Context, req *pb.ReplaceNodePrepareRequest) (resp *pb.ReplaceNodePrepareResponse, err error) {

	resp = &pb.ReplaceNodePrepareResponse{}

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
		AdminAddress: adminAddress,
	}

	err = withConnection(newStore, func(grpcConnection *grpc.ClientConn) error {

		client := pb.NewVastoStoreClient(grpcConnection)
		request := &pb.ReplicateNodeRequest{
			Keyspace:          req.Keyspace,
			ServerId:          req.NodeId,
			ClusterSize:       uint32(cluster.ExpectedSize()),
			ReplicationFactor: uint32(cluster.ReplicationFactor()),
		}

		log.Printf("replicate keyspace %s from %s to %v: %v", req.Keyspace, oldServer.GetAdminAddress(), newStore.AdminAddress, request)
		resp, err := client.ReplicateNode(ctx, request)
		if err != nil {
			return err
		}
		if resp.Error != "" {
			return fmt.Errorf("replicate keyspace %s from %s to %v: %s", req.Keyspace, oldServer.GetAdminAddress(), newStore.AdminAddress, resp.Error)
		}
		return nil
	});
	if err != nil {
		resp.Error = err.Error()
	}

	return resp, nil
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
