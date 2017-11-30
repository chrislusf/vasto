package store

import (
	"github.com/chrislusf/vasto/pb"
)

func (ss *storeServer) BootstrapCopy(request *pb.BootstrapCopyRequest, stream pb.VastoStore_BootstrapCopyServer) error {

	replica := ss.findDbReplica(request.NodeId)
	node := ss.nodes[replica]

	segment, offset := node.lm.GetSegmentOffset()

	// println("server", node.serverId, "node", node.id, "segment", segment, "offset", offset)

	err := node.db.FullScan(1024, func(rows []*pb.KeyValue) error {

		t := &pb.BootstrapCopyResponse{
			KeyValues: rows,
		}
		if err := stream.Send(t); err != nil {
			return err
		}
		return nil
	})

	t := &pb.BootstrapCopyResponse{
		BinlogTailProgress: &pb.BootstrapCopyResponse_BinlogTailProgress{
			Segment: segment,
			Offset:  uint64(offset),
		},
	}
	if err := stream.Send(t); err != nil {
		return err
	}

	return err
}

func (ss *storeServer) findDbReplica(nodeId uint32) (replica int) {
	for k, node := range ss.nodes {
		if node.id == int(nodeId) {
			replica = k
			return replica
		}
	}
	return -1
}
