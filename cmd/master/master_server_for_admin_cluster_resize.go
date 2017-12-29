package master

import (
	"fmt"
	"github.com/chrislusf/vasto/pb"
	"context"
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

	if cluster.GetNextClusterRing() != nil {
		resp.Error = fmt.Sprintf("cluster %s %s is resizing %d => %d in progress ...",
			req.Keyspace, req.DataCenter, cluster.CurrentSize(), cluster.GetNextClusterRing().ExpectedSize())
		return
	}

	if cluster.ExpectedSize() == int(req.GetClusterSize()) {
		resp.Error = fmt.Sprintf("cluster %s %s is already size %d", req.Keyspace, req.DataCenter, cluster.ExpectedSize())
		return
	}

	if cluster.ExpectedSize() < int(req.GetClusterSize()) {

		// grow the cluster

	} else {
		// shrink the cluster
	}

	return
}
