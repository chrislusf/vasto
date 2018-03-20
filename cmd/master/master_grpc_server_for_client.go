package master

import (
	"io"

	"fmt"
	"github.com/chrislusf/glog"
	"github.com/chrislusf/vasto/pb"
	"google.golang.org/grpc/peer"
	"net"
	"strings"
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

	serverAddress := serverAddress(pr.Addr.String())
	glog.V(0).Infof("+ client %v", serverAddress)

	// clean up if disconnects
	clientWatchedKeyspaceAndDataCenters := make(map[string]string)
	defer func() {
		for ksAndDc, clientName := range clientWatchedKeyspaceAndDataCenters {
			t := strings.Split(ksAndDc, ",")
			keyspace, dc := keyspaceName(t[0]), datacenterName(t[1])
			ms.clientChans.removeClient(keyspace, dc, serverAddress)
			ms.OnClientDisconnectEvent(dc, keyspace, serverAddress, clientName)
		}
		glog.V(0).Infof("- client %v", serverAddress)
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

		dc := datacenterName(clientHeartbeat.DataCenter)
		clientName := clientHeartbeat.ClientName

		if clientHeartbeat.GetClusterFollow() != nil {
			keyspace := keyspaceName(clientHeartbeat.ClusterFollow.Keyspace)
			clusterKey := fmt.Sprintf("%s,%s", keyspace, dc)

			if clientHeartbeat.ClusterFollow.IsUnfollow {
				delete(clientWatchedKeyspaceAndDataCenters, clusterKey)
				ms.clientChans.removeClient(keyspace, dc, serverAddress)
				ms.OnClientDisconnectEvent(dc, keyspace, serverAddress, clientName)
			} else {
				clientWatchedKeyspaceAndDataCenters[clusterKey] = clientName

				// for client, just set the expected cluster size to zero, and fix it when actual cluster is registered
				clusterRing, _ := ms.topo.keyspaces.getOrCreateKeyspace(string(keyspace)).doGetOrCreateCluster(string(dc), 0, 0)

				if ch, err := ms.clientChans.addClient(keyspace, dc, serverAddress); err == nil {
					// this is not added yet, start a goroutine that sends to the stream, until client disconnects
					ms.OnClientConnectEvent(dc, keyspace, serverAddress, clientName)
					go func() {
						ms.clientChans.sendClientCluster(keyspace, dc, serverAddress, clusterRing)
						for {
							select {
							case msg := <-ch:
								// fmt.Printf("master sends message %v\n", msg)
								if msg == nil {
									return
								}
								if err := stream.Send(msg); err != nil {
									glog.V(2).Infof("send to client %s message: %+v err: %v", serverAddress, msg, err)
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
