package master

import (
	"context"
	"fmt"
	"github.com/chrislusf/vasto/pb"
)

func (ms *masterServer) CompactCluster(ctx context.Context, req *pb.CompactClusterRequest) (resp *pb.CompactClusterResponse, err error) {

	ms.lock(req.Keyspace)
	defer ms.unlock(req.Keyspace)

	resp = &pb.CompactClusterResponse{}

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

	var servers []*pb.StoreResource
	for i := 0; i < cluster.ExpectedSize(); i++ {
		server, found := cluster.GetNode(i, 0)
		if !found {
			continue
		}
		servers = append(servers, server.GetStoreResource())
	}

	if err = compactShards(ctx, req, servers); err != nil {
		resp.Error = err.Error()
	}

	return resp, nil
}
