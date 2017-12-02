package master

import (
	"context"
	"fmt"
	"github.com/chrislusf/vasto/pb"
)

func (ms *masterServer) CreateCluster(ctx context.Context, req *pb.CreateClusterRequest) (resp *pb.CreateClusterResponse, err error) {

	resp = &pb.CreateClusterResponse{}

	dc, found := ms.topo.dataCenters.getDataCenter(req.DataCenter)
	if !found {
		resp.Error = fmt.Sprintf("no datacenter %v found", req.DataCenter)
		return
	}

	servers, err := dc.allocateServers(int(req.ClusterSize), float64(req.TotalDiskSizeGb*req.ReplicationFactor), req.Tags)
	var nodes []*pb.ClusterNode
	if err != nil {
		resp.Error = err.Error()
		return
	}

	for i, server := range servers {
		nodes = append(nodes, &pb.ClusterNode{
			StoreResource: &pb.StoreResource{
				Network:      server.Network,
				Address:      server.Address,
				AdminAddress: server.AdminAddress,
			},
			ShardStatus: &pb.ShardStatus{
				NodeId:  uint32(i),
				ShardId: uint32(i),
			},
		})
	}

	if err = dc.createShards(ctx, req, servers); err != nil {
		resp.Error = err.Error()
	}

	resp.Cluster = &pb.Cluster{
		Keyspace:            req.Keyspace,
		DataCenter:          req.DataCenter,
		Nodes:               nodes,
		ExpectedClusterSize: req.ClusterSize,
		CurrentClusterSize:  uint32(len(nodes)),
	}

	return resp, nil
}
