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

	fmt.Printf("client connected %v\n", pr.Addr.String())

	ch, err := ms.clientChans.addClient(clientHeartbeat.Location.DataCenter, pr.Addr.String())
	if err != nil {
		return err
	}

	ms.Lock()
	clusterRing, ok := ms.clusters[clientHeartbeat.Location.DataCenter]
	ms.Unlock()
	if ok {
		ms.clientChans.notifyCluster(
			clientHeartbeat.Location.DataCenter,
			clusterRing,
		)
	}

	clientDisconnectedChan := make(chan bool, 1)

	go func() {
		var e error
		for {
			_, e = stream.Recv()
			if e != nil {
				break
			}
		}
		ms.clientChans.removeClient(clientHeartbeat.Location.DataCenter, pr.Addr.String())
		fmt.Printf("client disconnected %v: %v\n", pr.Addr.String(), e)
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
