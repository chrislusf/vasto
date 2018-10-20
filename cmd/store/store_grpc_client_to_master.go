package store

import (
	"context"
	"fmt"
	"github.com/chrislusf/glog"
	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/util"
	"google.golang.org/grpc"
	"io"
	"strings"
	"time"
)

func (ss *storeServer) keepConnectedToMasterServer(ctx context.Context) {

	util.RetryForever(ctx, "store connect to master", func() error {
		return ss.registerAtMasterServer()
	}, 2*time.Second)

}

func (ss *storeServer) selfAddress() string {
	return fmt.Sprintf("%s:%d", *ss.option.Host, int32(*ss.option.TcpPort))
}

func (ss *storeServer) selfAdminAddress() string {
	return fmt.Sprintf("%s:%d", *ss.option.Host, ss.option.GetAdminPort())
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
		glog.Errorf("%s SendHeartbeat error: %v", ss.storeName, err)
		return err
	}

	glog.V(1).Infof("%s register store to master %s", ss.storeName, *ss.option.Master)

	storeHeartbeat := &pb.StoreHeartbeat{
		StoreResource: &pb.StoreResource{
			Network:      "tcp",
			Address:      ss.selfAddress(),
			AdminAddress: ss.selfAdminAddress(),
			DiskSizeGb:   uint32(*ss.option.DiskSizeGb),
			Tags:         strings.Split(*ss.option.Tags, ","),
		},
	}

	// glog.V(2).Infof("Reporting store %v", storeHeartbeat.StoreResource)

	if err := stream.Send(storeHeartbeat); err != nil {
		glog.Errorf("RegisterStore (%+v) = %v", storeHeartbeat, err)
		return err
	}

	// send any existing shard status to the master and quit
	go func() {
		// this is async, because
		// each sendShardInfoToMaster is listening on the ss.ShardInfoChan
		for _, storeStatus := range ss.statusInCluster {
			for _, shardInfo := range storeStatus.ShardMap {
				ss.sendShardInfoToMaster(shardInfo, pb.ShardInfo_READY)
			}
		}
	}()

	finishChan := make(chan bool)
	defer close(finishChan)

	go func() {
		for {
			select {
			case ShardInfo := <-ss.ShardInfoChan:
				// collect current server's different cluster shard status
				// log.Println("shard status => ", ShardInfo)
				storeHeartbeat = &pb.StoreHeartbeat{
					ShardInfo: ShardInfo,
				}
				if err := stream.Send(storeHeartbeat); err != nil {
					glog.Errorf("send shard status %v: %v", storeHeartbeat, err)
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

}

func (ss *storeServer) sendShardInfoToMaster(ShardInfo *pb.ShardInfo, status pb.ShardInfo_Status) {
	t := ShardInfo.Clone()
	t.Status = status
	glog.V(2).Infof("%s sending master: %v", ss.storeName, t)
	ss.ShardInfoChan <- t
}

func (ss *storeServer) processStoreMessage(msg *pb.StoreMessage) {
	glog.V(2).Infof("%s received message %v", ss.storeName, msg)
}
