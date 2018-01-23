package master

import (
	"context"
	"github.com/chrislusf/vasto/pb"
)

func (ms *masterServer) DebugMaster(ctx context.Context, req *pb.Empty) (*pb.Empty, error) {

	resp := &pb.Empty{}

	ms.topo.Debug()

	return resp, nil
}
