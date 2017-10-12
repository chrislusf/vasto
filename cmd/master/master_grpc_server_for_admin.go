package master

import (
	"context"
	"fmt"
	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/topology"
)

func (ms *masterServer) ListStores(ctx context.Context, req *pb.ListRequest) (*pb.ListResponse, error) {
	dc := req.DataCenter

	ms.Lock()
	r, _ := ms.rings[dc]
	ms.Unlock()

	stores := topology.ToStores(r)

	resp := &pb.ListResponse{
		DataCenter:  dc,
		ClusterSize: uint32(len(stores)),
		Stores:      stores,
	}
	return resp, nil
}

func (ms *masterServer) ResizeCluster(req *pb.ResizeRequest, stream pb.VastoMaster_ResizeClusterServer) error {

	dc := req.DataCenter

	ms.Lock()
	r, ok := ms.rings[dc]
	ms.Unlock()

	resp := &pb.ResizeProgress{}

	if !ok {
		resp.Error = fmt.Sprintf("data center %s not found", dc)
		if err := stream.Send(resp); err != nil {
			return err
		}
		return nil
	}

	if r.Size() == int(req.GetClusterSize()) {
		resp.Error = fmt.Sprintf("cluster %s is already size %d", dc, r.Size())
		if err := stream.Send(resp); err != nil {
			return err
		}
		return nil
	}

	if r.Size() > int(req.GetClusterSize()) {
		resp.Error = fmt.Sprintf("cluster %s size %d => %d downsizing is not supported yet.",
			dc, r.Size(), req.GetClusterSize())
		if err := stream.Send(resp); err != nil {
			return err
		}
		return nil
	}

	if r.Size() < int(req.GetClusterSize()) {
		resp.Progress = fmt.Sprintf("start cluster %s size %d => %d",
			dc, r.Size(), req.GetClusterSize())
		if err := stream.Send(resp); err != nil {
			return err
		}
	}

	ms.clientChans.notifyClusterSize(dc, uint32(r.Size()), req.GetClusterSize())
	ms.clientChans.notifyClusterSize(dc, req.GetClusterSize(), 0)

	return nil
}
