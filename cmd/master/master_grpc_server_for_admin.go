package master

import (
	"context"
	"fmt"
	"github.com/chrislusf/vasto/pb"
)

func (ms *masterServer) ListStores(ctx context.Context, req *pb.ListRequest) (*pb.ListResponse, error) {
	dc := req.DataCenter

	ms.Lock()
	r, _ := ms.clusters[dc]
	ms.Unlock()

	resp := &pb.ListResponse{
		Cluster:     r.ToCluster(),
		ClientCount: uint32(len(ms.clientChans.clientChans)),
	}
	return resp, nil
}

func (ms *masterServer) ResizeCluster(req *pb.ResizeRequest, stream pb.VastoMaster_ResizeClusterServer) error {

	dc := req.DataCenter

	ms.Lock()
	r, ok := ms.clusters[dc]
	ms.Unlock()

	resp := &pb.ResizeProgress{}

	if !ok {
		resp.Error = fmt.Sprintf("cluster %s not found", dc)
		if err := stream.Send(resp); err != nil {
			return err
		}
		return nil
	}

	if ms.nextClusterSize != 0 {
		resp.Error = fmt.Sprintf(
			"cluster %s is resizing %d => %d in progress ...",
			dc, ms.currentClusterSize, ms.nextClusterSize)
		if err := stream.Send(resp); err != nil {
			return err
		}
		return nil
	}

	if r.NodeCount() < int(req.GetClusterSize()) {
		resp.Error = fmt.Sprintf("cluster %s has size %d, less than requested %d", dc, r.NodeCount(), req.GetClusterSize())
		if err := stream.Send(resp); err != nil {
			return err
		}
		return nil
	}

	if ms.currentClusterSize == req.GetClusterSize() {
		resp.Error = fmt.Sprintf("cluster %s is already size %d", dc, ms.currentClusterSize)
		if err := stream.Send(resp); err != nil {
			return err
		}
		return nil
	} else if ms.currentClusterSize > req.GetClusterSize() {
		resp.Error = fmt.Sprintf("cluster %s size %d => %d downsizing is not supported yet.",
			dc, ms.currentClusterSize, req.GetClusterSize())
		if err := stream.Send(resp); err != nil {
			return err
		}
		return nil
	} else {
		resp.Progress = fmt.Sprintf("start cluster %s size %d => %d",
			dc, ms.currentClusterSize, req.GetClusterSize())
		if err := stream.Send(resp); err != nil {
			return err
		}
	}

	ms.nextClusterSize = req.GetClusterSize()
	ms.clientChans.notifyClusterSize(dc, ms.currentClusterSize, ms.nextClusterSize)

	ms.clientChans.notifyClusterSize(dc, ms.nextClusterSize, 0)
	ms.currentClusterSize = ms.nextClusterSize
	ms.nextClusterSize = 0

	return nil
}
