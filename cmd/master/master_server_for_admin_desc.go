package master

import (
	"context"
	"github.com/chrislusf/vasto/pb"
)

func (ms *masterServer) Describe(ctx context.Context, req *pb.DescribeRequest) (*pb.DescribeResponse, error) {

	resp := &pb.DescribeResponse{
		ClientCount: uint32(len(ms.clientChans.clientChans)),
	}

	if req.GetDescCluster() != nil {
		keyspace, found := ms.topo.keyspaces.getKeyspace(req.DescCluster.Keyspace)
		if found {
			cluster := keyspace.cluster
			if cluster != nil {
				resp.DescCluster = &pb.DescribeResponse_DescCluster{
					Cluster:     cluster.ToCluster(),
					ClientCount: uint32(ms.clientsStat.getKeyspaceClientCount(keyspace.name)),
				}
				if cluster.GetNextCluster() != nil {
					resp.DescCluster.NextCluster = cluster.GetNextCluster().ToCluster()
				}
			}
		}
	}
	if req.GetDescDataCenters() != nil {
		resp.DescDataCenter = &pb.DescribeResponse_DescDataCenter{}

		dataCenter := ms.topo.dataCenter
		var servers []*pb.StoreResource
		dataCenter.RLock()
		for _, server := range dataCenter.servers {
			t := server
			servers = append(servers, t)
		}
		dataCenter.RUnlock()
		resp.DescDataCenter.DataCenter = &pb.DescribeResponse_DescDataCenter_DataCenter{
			StoreResources: servers,
		}
	}

	if req.GetDescKeyspaces() != nil {
		resp.DescKeyspaces = &pb.DescribeResponse_DescKeyspaces{}
		ms.topo.keyspaces.RLock()
		for keyspaceName, keyspace := range ms.topo.keyspaces.keyspaces {
			var clusters []*pb.Cluster
			clusters = append(clusters, keyspace.cluster.ToCluster())
			resp.DescKeyspaces.Keyspaces = append(resp.DescKeyspaces.Keyspaces,
				&pb.DescribeResponse_DescKeyspaces_Keyspace{
					Keyspace:    string(keyspaceName),
					Clusters:    clusters,
					ClientCount: uint32(ms.clientsStat.getKeyspaceClientCount(keyspaceName)),
				})
		}
		ms.topo.keyspaces.RUnlock()
	}

	return resp, nil
}
