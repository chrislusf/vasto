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
)

func (ss *storeServer) keepConnectedToMasterServer() {

	util.RetryForever(func() error {
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
				int32(*ss.option.AdminPort),
			),
			StoreStatusesInCluster: ss.statusInCluster,
		},
	}

	// log.Printf("Reporting allocated %v", as.allocatedResource)

	if err := stream.Send(storeHeartbeat); err != nil {
		log.Printf("RegisterStore (%+v) = %v", storeHeartbeat, err)
		return err
	}

	finishChan := make(chan bool)
	go func() {
		select {
		case shardStatus := <-ss.shardStatusChan:
			// collect current server's different cluster node status
			log.Println("shard status => ", shardStatus)
			t, ok := storeHeartbeat.StoreResource.StoreStatusesInCluster[shardStatus.ClusterName]
			if !ok {
				t = &pb.StoreStatusInCluster{
					Id:           shardStatus.Id,
					NodeStatuses: make(map[uint32]*pb.ShardStatus),
				}
				storeHeartbeat.StoreResource.StoreStatusesInCluster[shardStatus.ClusterName] = t
			}
			t.NodeStatuses[shardStatus.Id] = shardStatus
			if err := stream.Send(storeHeartbeat); err != nil {
				log.Printf("send shard status: %v", storeHeartbeat, err)
				return
			}
		case <-finishChan:
			return
		}
	}()

	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			// read done.
			finishChan <- true
			return nil
		}
		if err != nil {
			finishChan <- true
			return fmt.Errorf("store receive topology : %v", err)
		}
		ss.processStoreMessage(msg)
	}

	return err

}

func (ss *storeServer) processStoreMessage(msg *pb.StoreMessage) {
	log.Printf("Received message %v", msg)
}
