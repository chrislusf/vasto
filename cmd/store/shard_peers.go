package store

import (
	"github.com/chrislusf/vasto/topology"
	"google.golang.org/grpc"
	"log"
	"context"
	"github.com/chrislusf/vasto/pb"
)

func (s *shard) isBootstrapNeeded(ctx context.Context, bootstrapOption *topology.BootstrapPlan) (bestPeerToCopy int, isNeeded bool) {

	peerShards := bootstrapOption.BootstrapSource

	isBootstrapNeededChan := make(chan bool, len(peerShards))
	maxSegment := uint32(0)
	bestPeerToCopy = -1
	checkedServerCount := 0
	for _, peer := range peerShards {
		_, _, ok := s.clusterRing.GetNode(peer.ServerId)
		if !ok {
			continue
		}
		checkedServerCount++
		go s.clusterRing.WithConnection(peer.ServerId, func(node *pb.ClusterNode, grpcConnection *grpc.ClientConn) error {

			latestSegment, canTailBinlog, err := s.checkBinlogAvailable(ctx, grpcConnection, node)
			if err != nil {
				isBootstrapNeededChan <- false
				return err
			}
			if latestSegment >= maxSegment {
				maxSegment = latestSegment
				bestPeerToCopy = peer.ServerId
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
