package store

import (
	"github.com/chrislusf/vasto/topology"
	"google.golang.org/grpc"
	"log"
	"context"
)

func (s *shard) isBootstrapNeeded(ctx context.Context) (bestPeerToCopy int, isNeeded bool) {

	peerServerIds := s.findPeerServerIds()

	isBootstrapNeededChan := make(chan bool, len(peerServerIds))
	maxSegment := uint32(0)
	bestPeerToCopy = -1
	checkedServerCount := 0
	for _, serverId := range peerServerIds {
		_, _, ok := s.clusterRing.GetNode(serverId)
		if !ok {
			continue
		}
		checkedServerCount++
		go s.clusterRing.WithConnection(serverId, func(node topology.Node, grpcConnection *grpc.ClientConn) error {

			latestSegment, canTailBinlog, err := s.checkBinlogAvailable(ctx, grpcConnection)
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

	if isNeeded {
		log.Printf("shard %v found peer %v to bootstrap from", s.id, bestPeerToCopy)
	} else {
		log.Printf("shard %v found bootstrapping is not needed", s.id)
	}

	return bestPeerToCopy, isNeeded
}

func (s *shard) findPeerServerIds() (serverIds []int) {

	size := s.clusterRing.ExpectedSize()

	for i := 0; i < s.replicationFactor && i < size; i++ {
		serverId := int(s.id) + i
		if serverId >= size {
			serverId -= size
		}
		if serverId == int(s.serverId) {
			continue
		}
		serverIds = append(serverIds, serverId)
	}

	log.Printf("cluster size %d, shard %d, server %d peers are: %v", s.clusterRing.ExpectedSize(), s.id, s.serverId, serverIds)

	return
}
