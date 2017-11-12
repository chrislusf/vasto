package store

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/chrislusf/vasto/pb"
	"google.golang.org/grpc"
)

func (ss *storeServer) TailBinlogFromStore(shardId int) error {

	storeLocation := "localhost:8279"

	grpcConnection, err := grpc.Dial(storeLocation, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("fail to dial: %v", err)
	}
	defer grpcConnection.Close()

	client := pb.NewVastoStoreClient(grpcConnection)

	ctx := context.Background()

	stream, err := client.BootstrapCopy(ctx)
	if err != nil {
		log.Printf("Copy error: %v", err)
		return err
	}

	log.Printf("copying to store %s", storeLocation)

	startTime := time.Now()
	log.Printf("copying starts at %v", startTime)

	// PrefixScan the whole table
	for {
		err := stream.Send(nil)
		if err != nil {
			return fmt.Errorf("copying key-value : %v", err)
		}
	}

	_, err = stream.CloseAndRecv()
	if err != nil {
		return fmt.Errorf("copy to shard %d %s : %v", shardId, storeLocation, err)
	}

	copyDoneMessage := &pb.CopyDoneMessge{
		Shard:           int32(shardId),
		CopyStartTimeNs: startTime.UnixNano(),
	}
	if _, err := client.CopyDone(ctx, copyDoneMessage); err != nil {
		log.Printf("CopyDone (%+v) = %v", copyDoneMessage, err)
		return err
	}

	return nil

}

func (ss *storeServer) copyToStore(shardId int) error {

	storeLocation := "localhost:8279"

	grpcConnection, err := grpc.Dial(storeLocation, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("fail to dial: %v", err)
	}
	defer grpcConnection.Close()

	client := pb.NewVastoStoreClient(grpcConnection)

	ctx := context.Background()

	stream, err := client.BootstrapCopy(ctx)
	if err != nil {
		log.Printf("Copy error: %v", err)
		return err
	}

	log.Printf("copying to store %s", storeLocation)

	startTime := time.Now()
	log.Printf("copying starts at %v", startTime)

	// PrefixScan the whole table
	for {
		err := stream.Send(nil)
		if err != nil {
			return fmt.Errorf("copying key-value : %v", err)
		}
	}

	_, err = stream.CloseAndRecv()
	if err != nil {
		return fmt.Errorf("copy to shard %d %s : %v", shardId, storeLocation, err)
	}

	copyDoneMessage := &pb.CopyDoneMessge{
		Shard:           int32(shardId),
		CopyStartTimeNs: startTime.UnixNano(),
	}
	if _, err := client.CopyDone(ctx, copyDoneMessage); err != nil {
		log.Printf("CopyDone (%+v) = %v", copyDoneMessage, err)
		return err
	}

	return nil

}
