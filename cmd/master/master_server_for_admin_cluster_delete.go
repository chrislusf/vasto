package master

import (
	"context"
	"fmt"
	"github.com/chrislusf/vasto/pb"
)

func (ms *masterServer) DeleteCluster(ctx context.Context, req *pb.DeleteClusterRequest) (resp *pb.DeleteClusterResponse, err error) {

	ms.lock(req.Keyspace)
	defer ms.unlock(req.Keyspace)

	resp = &pb.DeleteClusterResponse{}

	keyspace, found := ms.topo.keyspaces.getKeyspace(req.Keyspace)
	if !found {
		resp.Error = fmt.Sprintf("no keyspace %v found", req.Keyspace)
		return
	}

	cluster := keyspace.cluster
	if cluster == nil {
		resp.Error = fmt.Sprintf("no cluster for %v found", req.Keyspace)
		return
	}

	var servers []*pb.StoreResource
	for i := 0; i < cluster.ExpectedSize(); i++ {
		server, found := cluster.GetNode(i, 0)
		if !found {
			continue
		}
		servers = append(servers, server.GetStoreResource())
	}

	if err = deleteShards(ctx, req, servers); err != nil {
		resp.Error = err.Error()
	}

	return resp, nil
}
