package master

import (
	"io"

	"fmt"
	"github.com/chrislusf/glog"
	"github.com/chrislusf/vasto/pb"
	"google.golang.org/grpc/peer"
	"net"
)

func (ms *masterServer) RegisterClient(stream pb.VastoMaster_RegisterClientServer) error {

	// remember client address
	ctx := stream.Context()
	// fmt.Printf("FromContext %+v\n", ctx)
	pr, ok := peer.FromContext(ctx)
	if !ok {
		glog.Error("failed to get peer from ctx")
		return fmt.Errorf("failed to get peer from ctx")
	}
	if pr.Addr == net.Addr(nil) {
		glog.Error("failed to get peer address")
		return fmt.Errorf("failed to get peer address")
	}

	clientAddress := serverAddress(pr.Addr.String())
	glog.V(1).Infof("+ client %v", clientAddress)

	// clean up if disconnects
	clientWatchedKeyspaces := make(map[keyspaceName]string)
	defer func() {
		for keyspace, clientName := range clientWatchedKeyspaces {
			ms.clientChans.removeClient(keyspace, clientAddress)
			ms.OnClientDisconnectEvent(keyspace, clientAddress, clientName)
		}
		glog.V(1).Infof("- client %v", clientAddress)
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

		clientName := clientHeartbeat.ClientName

		if clientHeartbeat.GetClusterFollow() != nil {
			keyspace := keyspaceName(clientHeartbeat.ClusterFollow.Keyspace)

			if clientHeartbeat.ClusterFollow.IsUnfollow {
				ms.clientChans.removeClient(keyspace, clientAddress)
				ms.OnClientDisconnectEvent(keyspace, clientAddress, clientName)
			} else {
				clientWatchedKeyspaces[keyspace] = clientName

				// for client, just set the expected cluster size to zero, and fix it when actual cluster is registered
				clusterRing, _ := ms.topo.keyspaces.getOrCreateKeyspace(string(keyspace)).doGetOrCreateCluster(0, 0)

				if ch, err := ms.clientChans.addClient(keyspace, clientAddress); err == nil {
					// this is not added yet, start a goroutine that sends to the stream, until client disconnects
					ms.OnClientConnectEvent(keyspace, clientAddress, clientName)
					go func() {
						ms.clientChans.sendClientCluster(keyspace, clientAddress, clusterRing)
						for {
							select {
							case msg := <-ch:
								// fmt.Printf("master sends message %v\n", msg)
								if msg == nil {
									return
								}
								if err := stream.Send(msg); err != nil {
									glog.V(2).Infof("send to client %s message: %+v err: %v", clientAddress, msg, err)
									return
								}
							case <-clientDisconnectedChan:
								return
							}
						}
					}()
				} else {
					glog.Errorf("master add client %s: %v", clientName, err)
				}

			}
		}
	}

}
