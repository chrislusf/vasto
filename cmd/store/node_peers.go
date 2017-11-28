package store

import (
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
		_, _, ok := n.clusterRing.GetNode(serverId)
		if !ok {
			continue
		}
		checkedServerCount++
		go n.clusterRing.WithConnection(serverId, func(node topology.Node, grpcConnection *grpc.ClientConn) error {

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

	size := n.clusterRing.ExpectedSize()

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
