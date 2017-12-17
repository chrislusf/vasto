package store

import (
	"context"
	"fmt"
	"log"

	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/util"
	"google.golang.org/grpc"
	"io"
	"time"
	"strings"
)

func (ss *storeServer) keepConnectedToMasterServer(ctx context.Context) {

	util.RetryForever(ctx, "store connect to master", func() error {
		return ss.registerAtMasterServer()
	}, 2*time.Second)

}

func (ss *storeServer) registerAtMasterServer() error {
	grpcConnection, err := grpc.Dial(*ss.option.Master, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("fail to dial: %v", err)
	}
	defer grpcConnection.Close()

	client := pb.NewVastoMasterClient(grpcConnection)

	stream, err := client.RegisterStore(context.Background())
	if err != nil {
		log.Printf("SendHeartbeat error: %v", err)
		return err
	}

	log.Printf("register store to master %s", *ss.option.Master)

	storeHeartbeat := &pb.StoreHeartbeat{
		StoreResource: &pb.StoreResource{
			DataCenter: *ss.option.DataCenter,
			Network:    "tcp",
			Address: fmt.Sprintf(
				"%s:%d",
				*ss.option.Host,
				int32(*ss.option.TcpPort),
			),
			AdminAddress: fmt.Sprintf(
				"%s:%d",
				*ss.option.Host,
				ss.option.GetAdminPort(),
			),
			DiskSizeGb: uint32(*ss.option.DiskSizeGb),
			Tags:       strings.Split(*ss.option.Tags, ","),
		},
	}

	// log.Printf("Reporting store %v", storeHeartbeat.StoreResource)

	if err := stream.Send(storeHeartbeat); err != nil {
		log.Printf("RegisterStore (%+v) = %v", storeHeartbeat, err)
		return err
	}

	// send any existing shard status to the master and quit
	go func() {
		// this is async, because
		// each sendShardStatusToMaster is listening on the ss.shardStatusChan
		for _, storeStatus := range ss.statusInCluster {
			for _, shardStatus := range storeStatus.ShardStatuses {
				ss.sendShardStatusToMaster(shardStatus)
			}
		}
	}()

	finishChan := make(chan bool)
	defer close(finishChan)

	go func() {
		for {
			select {
			case shardStatus := <-ss.shardStatusChan:
				// collect current server's different cluster shard status
				// log.Println("shard status => ", shardStatus)
				storeHeartbeat = &pb.StoreHeartbeat{
					ShardStatus: shardStatus,
				}
				if err := stream.Send(storeHeartbeat); err != nil {
					log.Printf("send shard status: %v", storeHeartbeat, err)
					return
				}
			case <-finishChan:
				return
			}
		}
	}()

	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			// read done.
			return nil
		}
		if err != nil {
			return fmt.Errorf("store receive topology : %v", err)
		}
		ss.processStoreMessage(msg)
	}

	return err

}

func (ss *storeServer) sendShardStatusToMaster(status *pb.ShardStatus) {
	log.Printf("Sending master: %v", status)
	ss.shardStatusChan <- status
}

func (ss *storeServer) processStoreMessage(msg *pb.StoreMessage) {
	log.Printf("Received message %v", msg)
}
