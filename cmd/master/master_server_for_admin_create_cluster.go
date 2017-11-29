package master

import (
	"context"
	"fmt"
	"github.com/chrislusf/vasto/pb"
)

func (ms *masterServer) CreateCluster(ctx context.Context, req *pb.CreateClusterRequest) (resp *pb.CreateClusterResponse, err error) {

	resp = &pb.CreateClusterResponse{}

	_, found := ms.topo.dataCenters.getDataCenter(req.DataCenter)
	if !found {
		resp.Error = fmt.Sprintf("no datacenter %v found", req.DataCenter)
		return
	}

	_, found = ms.topo.keyspaces.getKeyspace(req.DataCenter)
	if found {
		resp.Error = fmt.Sprintf("keyspace %v found", req.DataCenter)
		return
	}

	return resp, nil
}
