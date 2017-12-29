package master

import (
	"context"
	"fmt"
	"github.com/chrislusf/vasto/pb"
	"math"
)

func (ms *masterServer) CreateCluster(ctx context.Context, req *pb.CreateClusterRequest) (resp *pb.CreateClusterResponse, err error) {

	resp = &pb.CreateClusterResponse{}

	dc, found := ms.topo.dataCenters.getDataCenter(req.DataCenter)
	if !found {
		resp.Error = fmt.Sprintf("no datacenter %v found", req.DataCenter)
		return
	}

	servers, err := dc.allocateServers(int(req.ClusterSize), float64(req.TotalDiskSizeGb*req.ReplicationFactor),
		func(resource *pb.StoreResource) bool {
			return meetRequirement(resource.Tags, req.Tags)
		})
	if err != nil {
		resp.Error = err.Error()
		return
	}

	var nodes []*pb.ClusterNode
	for i, server := range servers {
		nodes = append(nodes, &pb.ClusterNode{
			StoreResource: &pb.StoreResource{
				Network:      server.Network,
				Address:      server.Address,
				AdminAddress: server.AdminAddress,
			},
			ShardInfo: &pb.ShardInfo{
				NodeId:  uint32(i),
				ShardId: uint32(i),
			},
		})
	}

	eachShardSizeGb := uint32(math.Ceil(float64(req.TotalDiskSizeGb) / float64(req.ClusterSize)))

	if err = createShards(ctx, req.Keyspace, req.ClusterSize, req.ReplicationFactor, eachShardSizeGb, servers, 0); err != nil {
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
