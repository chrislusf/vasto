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
			cluster, found := keyspace.getCluster(req.DescCluster.DataCenter)
			if found {
				resp.DescCluster = &pb.DescribeResponse_DescCluster{
					Cluster:     cluster.ToCluster(),
					ClientCount: uint32(ms.clientsStat.getClusterClientCount(keyspace.name, data_center_name(req.DescCluster.DataCenter))),
				}
			}
		}
	}
	if req.GetDescDataCenters() != nil {
		resp.DescDataCenters = &pb.DescribeResponse_DescDataCenters{}
		ms.topo.dataCenters.RLock()
		for dataCenterName, dataCenter := range ms.topo.dataCenters.dataCenters {
			var servers []*pb.StoreResource
			dataCenter.RLock()
			for _, server := range dataCenter.servers {
				t := server
				servers = append(servers, t)
			}
			dataCenter.RUnlock()
			resp.DescDataCenters.DataCenters = append(resp.DescDataCenters.DataCenters,
				&pb.DescribeResponse_DescDataCenters_DataCenter{
					DataCenter:     string(dataCenterName),
					StoreResources: servers,
					ClientCount:    uint32(ms.clientsStat.getDataCenterClientCount(dataCenterName)),
				})
		}
		ms.topo.dataCenters.RUnlock()
	}

	if req.GetDescKeyspaces() != nil {
		resp.DescKeyspaces = &pb.DescribeResponse_DescKeyspaces{}
		ms.topo.keyspaces.RLock()
		for keyspaceName, keyspace := range ms.topo.keyspaces.keyspaces {
			var clusters []*pb.Cluster
			keyspace.RLock()
			for _, cluster := range keyspace.clusters {
				clusters = append(clusters, cluster.ToCluster())
			}
			keyspace.RUnlock()
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
