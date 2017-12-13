package cluster_listener

import (
	"context"
	"fmt"
	"io"
	"log"

	"github.com/chrislusf/vasto/pb"
	"google.golang.org/grpc"
)

func (clusterListener *ClusterListener) registerClientAtMasterServer(master string, dataCenter string,
	msgChan chan *pb.ClientMessage) error {
	grpcConnection, err := grpc.Dial(master, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("fail to dial %s: %v", master, err)
	}
	defer grpcConnection.Close()

	masterClient := pb.NewVastoMasterClient(grpcConnection)

	stream, err := masterClient.RegisterClient(context.Background())
	if err != nil {
		return fmt.Errorf("register client on master %v: %v", master, err)
	}

	go func() {
		for keyspace, _ := range clusterListener.clusters {
			log.Printf("register cluster keyspace(%v) datacenter(%v)", keyspace, dataCenter)
			if err := registerForClusterAtMaster(stream, string(keyspace), dataCenter, false); err != nil {
				log.Printf("register cluster keyspace(%v) datacenter(%v): %v", keyspace, dataCenter, err)
				return
			}
		}

		for {
			msg := <-clusterListener.keyspaceFollowMessageChan
			if msg.isUnfollow {
				log.Printf("unfollow cluster keyspace(%v) datacenter(%v)", msg.keyspace, dataCenter)
			} else {
				log.Printf("register cluster new keyspace(%v) datacenter(%v)", msg.keyspace, dataCenter)
			}
			if err := registerForClusterAtMaster(stream, string(msg.keyspace), dataCenter, msg.isUnfollow); err != nil {
				if msg.isUnfollow {
					log.Printf("unfollow cluster keyspace(%v) datacenter(%v): %v", msg.keyspace, dataCenter, err)
				} else {
					log.Printf("register cluster new keyspace(%v) datacenter(%v): %v", msg.keyspace, dataCenter, err)
				}
				return
			}
		}

	}()

	// log.Printf("Reporting allocated %v", as.allocatedResource)

	log.Printf("register client to master %s", master)

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
		// log.Printf("client received message %v", msg)
	}

}

func registerForClusterAtMaster(stream pb.VastoMaster_RegisterClientClient, keyspace, dataCenter string, isUnfollow bool) error {
	clientHeartbeat := &pb.ClientHeartbeat{
		DataCenter: dataCenter,
		ClusterFollow: &pb.ClientHeartbeat_ClusterFollowMessage{
			Keyspace:   keyspace,
			IsUnfollow: isUnfollow,
		},
	}

	if err := stream.Send(clientHeartbeat); err != nil {
		return fmt.Errorf("client send heartbeat: %v", err)
	}
	return nil
}
