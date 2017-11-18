package store

import (
	"fmt"
	"log"

	"github.com/chrislusf/vasto/topology"
	"google.golang.org/grpc"
)

func (n *node) isBootstrapNeeded() (bestPeerToCopy int, isNeeded bool) {

	peerServerIds := n.findPeerServerIds()

	isBootstrapNeededChan := make(chan bool, len(peerServerIds))
	maxSegment := uint32(0)
	bestPeerToCopy = -1
	checkedServerCount := 0
	for _, serverId := range peerServerIds {
		_, _, ok := n.clusterListener.GetNode(serverId)
		if !ok {
			continue
		}
		checkedServerCount++
		go n.withConnection(serverId, func(node topology.Node, grpcConnection *grpc.ClientConn) error {

			latestSegment, canTailBinlog, err := n.checkBinlogAvailable(grpcConnection)
			if err != nil {
				isBootstrapNeededChan <- false
				return err
			}
			if latestSegment >= maxSegment {
				maxSegment = latestSegment
				bestPeerToCopy = serverId
			}
			isBootstrapNeededChan <- !canTailBinlog
			return nil
		})
	}

	for i := 0; i < checkedServerCount; i++ {
		t := <-isBootstrapNeededChan
		isNeeded = isNeeded || t
	}

	return bestPeerToCopy, isNeeded
}

func (n *node) findPeerServerIds() (serverIds []int) {

	size := n.clusterListener.ExpectedSize()

	for i := 0; i < n.replicationFactor && i < size; i++ {
		serverId := n.id + i
		if serverId >= size {
			serverId -= size
		}
		if serverId == n.serverId {
			continue
		}
		serverIds = append(serverIds, serverId)
	}

	// log.Printf("cluster size %d, node %d, server %d peers are: %v", n.clusterListener.ExpectedSize(), n.id, n.serverId, serverIds)

	return
}

func (n *node) withConnection(serverId int, fn func(topology.Node, *grpc.ClientConn) error) error {

	node, _, ok := n.clusterListener.GetNode(serverId)

	if !ok {
		return fmt.Errorf("server %d not found", serverId)
	}

	if node == nil {
		return fmt.Errorf("server %d is missing", serverId)
	}

	// log.Printf("connecting to server %d at %s", serverId, node.GetAdminAddress())

	grpcConnection, err := grpc.Dial(node.GetAdminAddress(), grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("fail to dial %s: %v", node.GetAdminAddress(), err)
	}
	defer grpcConnection.Close()

	log.Printf("node %d connected to server %d at %s", node.GetId(), serverId, node.GetAdminAddress())

	return fn(node, grpcConnection)
}
