package store

import (
	"fmt"
	"io"
	"log"
	"net"

	"github.com/chrislusf/vasto/pb"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

func (ss *storeServer) serveGrpc(listener net.Listener) {
	grpcServer := grpc.NewServer()
	pb.RegisterVastoStoreServer(grpcServer, ss)
	grpcServer.Serve(listener)
}

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

func (ss *storeServer) TailBinlog(request *pb.PullUpdateRequest, stream pb.VastoStore_TailBinlogServer) error {

	replica := ss.findDbReplica(request.NodeId)
	segment := uint32(request.Segment)
	offset := int64(request.Offset)
	limit := int(request.Limit)

	// println("TailBinlog server, segment", segment, "offset", offset, "limit", limit)

	if !ss.nodes[replica].lm.HasSegment(segment) {

		t := &pb.PullUpdateResponse{
			OutOfSync: true,
		}

		if err := stream.Send(t); err != nil {
			return err
		}

		start, stop := ss.nodes[replica].lm.GetSegmentRange()

		return fmt.Errorf("out of sync client reads segment %d offset %d, only has segment [%d,%d]",
			segment, offset, start, stop)

	}

	for {

		// println("TailBinlog server reading entries, segment", segment, "offset", offset, "limit", limit)

		entries, nextOffset, err := ss.nodes[replica].lm.ReadEntries(segment, offset, limit)
		if err == io.EOF {
			segment += 1
		} else if err != nil {
			return fmt.Errorf("failed to read segment %d offset %d: %v", segment, offset, err)
		}
		// println("len(entries) =", len(entries), "offset", offset, "next offset", nextOffset)

		offset = nextOffset

		t := &pb.PullUpdateResponse{
			NextSegment: segment,
			NextOffset:  uint64(nextOffset),
		}

		for _, entry := range entries {
			if !entry.IsValid() {
				log.Printf("read an invalid entry: %+v", entry)
				continue
			}
			t.Entries = append(t.Entries, &pb.UpdateEntry{
				PartitionHash: entry.PartitionHash,
				UpdatedAtNs:   entry.UpdatedNanoSeconds,
				TtlSecond:     entry.TtlSecond,
				IsDelete:      entry.IsDelete,
				Key:           entry.Key,
				Value:         entry.Value,
			})
		}

		if err := stream.Send(t); err != nil {
			return err
		}

	}

	return nil
}

func (ss *storeServer) CheckBinlog(ctx context.Context, request *pb.CheckBinlogRequest) (*pb.CheckBinlogResponse, error) {
	replica := ss.findDbReplica(request.NodeId)

	earliestSegment, latestSegment := ss.nodes[replica].lm.GetSegmentRange()

	return &pb.CheckBinlogResponse{
		NodeId:          request.NodeId,
		EarliestSegment: earliestSegment,
		LatestSegment:   latestSegment,
	}, nil

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
