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

	if keyspace.cluster == nil {
		resp.Error = fmt.Sprintf("no cluster %v created", req.Keyspace)
		return
	}

	var servers []*pb.StoreResource
	for i := 0; i < keyspace.cluster.ExpectedSize(); i++ {
		server, found := keyspace.cluster.GetNode(i, 0)
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
