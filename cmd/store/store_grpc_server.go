package store

import (
	"net"

	"fmt"
	"github.com/chrislusf/vasto/pb"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"io"
	"log"
)

func (ss *storeServer) serveGrpc(listener net.Listener) {
	grpcServer := grpc.NewServer()
	pb.RegisterVastoStoreServer(grpcServer, ss)
	grpcServer.Serve(listener)
}

func (ss *storeServer) Copy(stream pb.VastoStore_CopyServer) error {
	return nil
}

func (ss *storeServer) PullChanges(request *pb.PullUpdateRequest, stream pb.VastoStore_PullChangesServer) error {
	segment := uint16(request.Segment)
	offset := int64(request.Offset)
	limit := int(request.Limit)

	for {

		entries, nextOffset, err := ss.lm.ReadEntries(segment, offset, limit)
		if err == io.EOF {
			segment += 1
		} else if err != nil {
			return fmt.Errorf("failed to read segment %d offset %d: %v", segment, offset, err)
		}
		offset = nextOffset

		t := &pb.PullUpdateResponse{
			NextSegment: uint32(segment),
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

func (ss *storeServer) CopyDone(ctx context.Context, request *pb.CopyDoneMessge) (*pb.Empty, error) {
	return &pb.Empty{}, nil
}
