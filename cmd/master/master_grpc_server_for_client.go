package master

import (
	"io"

	"fmt"
	"github.com/chrislusf/vasto/pb"
	"google.golang.org/grpc/peer"
	"log"
	"net"
)

func (ms *masterServer) RegisterClient(stream pb.VastoMaster_RegisterClientServer) error {

	clientHeartbeat, err := stream.Recv()
	if err == io.EOF {
		return nil
	}
	if err != nil {
		return err
	}

	ctx := stream.Context()
	// fmt.Printf("FromContext %+v\n", ctx)
	pr, ok := peer.FromContext(ctx)
	if !ok {
		log.Println("failed to get peer from ctx")
		return fmt.Errorf("failed to get peer from ctx")
	}
	if pr.Addr == net.Addr(nil) {
		log.Println("failed to get peer address")
		return fmt.Errorf("failed to get peer address")
	}

	clusterRing, found := ms.topo.keyspaces.getOrCreateKeyspace(clientHeartbeat.Keyspace).getCluster(
		clientHeartbeat.DataCenter,
	)

	if !found {
		return fmt.Errorf("keyspace %s in datacenter %s not found",
			clientHeartbeat.Keyspace, clientHeartbeat.DataCenter)
	}

	log.Printf("client connected %v\n", pr.Addr.String())

	ch, err := ms.clientChans.addClient(clientHeartbeat.Keyspace, clientHeartbeat.DataCenter, pr.Addr.String())
	if err != nil {
		return err
	}

	ms.clientChans.sendClientCluster(
		clientHeartbeat.Keyspace,
		clientHeartbeat.DataCenter,
		pr.Addr.String(),
		clusterRing,
	)

	clientDisconnectedChan := make(chan bool, 1)

	go func() {
		var e error
		for {
			_, e = stream.Recv()
			if e != nil {
				break
			}
		}
		ms.clientChans.removeClient(clientHeartbeat.Keyspace, clientHeartbeat.DataCenter, pr.Addr.String())
		log.Printf("client disconnected %v: %v", pr.Addr.String(), e)
		clientDisconnectedChan <- true
	}()

	for {
		select {
		case msg := <-ch:
			// fmt.Printf("master received message %v\n", msg)
			if err := stream.Send(msg); err != nil {
				return err
			}
		case <-clientDisconnectedChan:
			return nil
		}
	}

	return nil
}
