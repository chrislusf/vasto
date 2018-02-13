package cluster_listener

import (
	"context"
	"fmt"
	"io"

	"github.com/chrislusf/vasto/pb"
	"google.golang.org/grpc"
)

func (clusterListener *ClusterListener) registerClientAtMasterServer(master string, dataCenter string,
	msgChan chan *pb.ClientMessage) error {
	grpcConnection, err := grpc.Dial(master, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("%s fail to dial %s: %v", clusterListener.clientName, master, err)
	}
	defer grpcConnection.Close()

	masterClient := pb.NewVastoMasterClient(grpcConnection)

	stream, err := masterClient.RegisterClient(context.Background())
	if err != nil {
		return fmt.Errorf("%s register client on master %v: %v", clusterListener.clientName, master, err)
	}

	// TODO possible goroutine leaks if retry happens
	go func() {
		for keyspace, _ := range clusterListener.clusters {
			// glog.V(2).Infof("%s register cluster keyspace(%v) datacenter(%v)", clusterListener.clientName, keyspace, dataCenter)
			if err := registerForClusterAtMaster(stream, string(keyspace), dataCenter, false, clusterListener.clientName); err != nil {
				// glog.V(2).Infof("%s register cluster keyspace(%v) datacenter(%v): %v", clusterListener.clientName, keyspace, dataCenter, err)
				return
			}
		}

		for {
			msg := <-clusterListener.keyspaceFollowMessageChan
			if msg.isUnfollow {
				// glog.V(2).Infof("%s unfollow cluster keyspace(%v) datacenter(%v)", clusterListener.clientName, msg.keyspace, dataCenter)
			} else {
				// glog.V(2).Infof("%s register cluster new keyspace(%v) datacenter(%v)", clusterListener.clientName, msg.keyspace, dataCenter)
			}
			if err := registerForClusterAtMaster(stream, string(msg.keyspace), dataCenter, msg.isUnfollow, clusterListener.clientName); err != nil {
				if msg.isUnfollow {
					// glog.V(2).Infof("%s unfollow cluster keyspace(%v) datacenter(%v): %v", clusterListener.clientName, msg.keyspace, dataCenter, err)
				} else {
					// glog.V(2).Infof("%s register cluster new keyspace(%v) datacenter(%v): %v", clusterListener.clientName, msg.keyspace, dataCenter, err)
				}
				return
			}
		}

	}()

	// glog.V(2).Infof("Reporting allocated %v", as.allocatedResource)

	// glog.V(2).Infof("%s from %s register client to master %s", clusterListener.clientName, dataCenter, master)

	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			// read done.
			return nil
		}
		if err != nil {
			return fmt.Errorf("client receive topology : %v", err)
		}
		msgChan <- msg
		// glog.V(2).Infof("%s client received message %v", clusterListener.clientName, msg)
	}

}

func registerForClusterAtMaster(stream pb.VastoMaster_RegisterClientClient, keyspace, dataCenter string, isUnfollow bool, clientName string) error {
	clientHeartbeat := &pb.ClientHeartbeat{
		DataCenter: dataCenter,
		ClientName: clientName,
		ClusterFollow: &pb.ClientHeartbeat_ClusterFollowMessage{
			Keyspace:   keyspace,
			IsUnfollow: isUnfollow,
		},
	}

	if err := stream.Send(clientHeartbeat); err != nil {
		return fmt.Errorf("%s client send heartbeat: %v", clientName, err)
	}
	return nil
}
