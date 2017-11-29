package master

import (
	"context"
	"fmt"
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
					Cluster: cluster.ToCluster(),
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
					Keyspace: string(keyspaceName),
					Clusters: clusters,
				})
		}
		ms.topo.keyspaces.RUnlock()
	}

	return resp, nil
}

func (ms *masterServer) ResizeCluster(req *pb.ResizeRequest, stream pb.VastoMaster_ResizeClusterServer) error {

	keyspace, dc := req.Keyspace, req.DataCenter

	r, found := ms.topo.keyspaces.getOrCreateKeyspace(keyspace).getCluster(dc)

	resp := &pb.ResizeProgress{}

	if !found {
		resp.Error = fmt.Sprintf("cluster %s not found", dc)
		if err := stream.Send(resp); err != nil {
			return err
		}
		return nil
	}

	if r.NextSize() != 0 {
		resp.Error = fmt.Sprintf(
			"cluster %s is resizing %d => %d in progress ...",
			dc, r.CurrentSize(), r.NextSize())
		if err := stream.Send(resp); err != nil {
			return err
		}
		return nil
	}

	if r.CurrentSize() < int(req.GetClusterSize()) {
		resp.Error = fmt.Sprintf("cluster %s has size %d, less than requested %d", dc, r.CurrentSize(), req.GetClusterSize())
		if err := stream.Send(resp); err != nil {
			return err
		}
		return nil
	}

	if r.CurrentSize() == int(req.GetClusterSize()) {
		resp.Error = fmt.Sprintf("cluster %s is already size %d", dc, r.CurrentSize())
		if err := stream.Send(resp); err != nil {
			return err
		}
		return nil
	} else if r.CurrentSize() > int(req.GetClusterSize()) {
		resp.Error = fmt.Sprintf("cluster %s size %d => %d downsizing is not supported yet.",
			dc, r.CurrentSize(), req.GetClusterSize())
		if err := stream.Send(resp); err != nil {
			return err
		}
		return nil
	} else {
		resp.Progress = fmt.Sprintf("start cluster %s size %d => %d",
			dc, r.CurrentSize(), req.GetClusterSize())
		if err := stream.Send(resp); err != nil {
			return err
		}
	}

	r.SetNextSize(int(req.GetClusterSize()))
	ms.clientChans.notifyClusterSize(keyspace, dc, uint32(r.CurrentSize()), uint32(r.NextSize()))

	ms.clientChans.notifyClusterSize(keyspace, dc, uint32(r.NextSize()), 0)
	r.SetExpectedSize(r.NextSize())
	r.SetNextSize(0)

	return nil
}
