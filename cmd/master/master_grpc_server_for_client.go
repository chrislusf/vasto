package master

import (
	"io"

	"fmt"
	"github.com/chrislusf/vasto/pb"
	"google.golang.org/grpc/peer"
	"log"
	"net"
	"strings"
)

func (ms *masterServer) RegisterClient(stream pb.VastoMaster_RegisterClientServer) error {

	// remember client address
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
	log.Printf("client connected %v\n", pr.Addr.String())

	// clean up if disconnects
	clientWatchedKeyspaceAndDataCenters := make(map[string]bool)
	defer func() {
		for k_dc, _ := range clientWatchedKeyspaceAndDataCenters {
			t := strings.Split(k_dc, ",")
			keyspace, dc := t[0], t[1]
			ms.clientChans.removeClient(keyspace, dc, pr.Addr.String())
			log.Printf("client %v disconnect from cluster keyspace(%v) datacenter(%v)\n",
				pr.Addr.String(), keyspace, dc)
		}
		log.Printf("client disconnected %v", pr.Addr.String())
	}()

	// the channel is used to stop spawned goroutines
	clientDisconnectedChan := make(chan bool)
	defer close(clientDisconnectedChan)

	for {
		clientHeartbeat, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read from client %v", err)
		}

		dc := clientHeartbeat.DataCenter
		keyspace := clientHeartbeat.Keyspace
		clientWatchedKeyspaceAndDataCenters[keyspace+","+dc] = true

		clusterRing, _ := ms.topo.keyspaces.getOrCreateKeyspace(keyspace).doGetOrCreateCluster(dc)

		if ch, err := ms.clientChans.addClient(keyspace, dc, pr.Addr.String()); err == nil {
			// this is not added yet, start a goroutine that sends to the stream, until client disconnects
			log.Printf("client %v connect to cluster keyspace(%v) datacenter(%v)\n",
				pr.Addr.String(), keyspace, dc)
			go func() {
				ms.clientChans.sendClientCluster(
					keyspace,
					dc,
					pr.Addr.String(),
					clusterRing,
				)
				for {
					select {
					case msg := <-ch:
						// fmt.Printf("master received message %v\n", msg)
						if err := stream.Send(msg); err != nil {
							return
						}
					case <-clientDisconnectedChan:
						return
					}
				}
			}()
		}

	}

	return nil
}
