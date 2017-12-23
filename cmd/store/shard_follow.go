package store

import (
	"time"

	"github.com/chrislusf/vasto/topology"
	"github.com/chrislusf/vasto/util"
	"google.golang.org/grpc"
	"fmt"
	"context"
	"log"
)

// follow keep trying all peers in the cluster and keep retrying to follow the peers
func (s *shard) follow(ctx context.Context, selfAdminAddress string) {

	for _, peer := range s.peerShards() {
		sid := peer.ServerId
		go util.RetryForever(ctx, fmt.Sprintf("server %d shard %d to server %d", s.serverId, s.id, sid), func() error {
			return s.doFollow(ctx, sid)
		}, 2*time.Second)
	}

	// follow the shard on the to-be-replaced server when the CANDIDATE shard starts
	mainShard, _, found := s.clusterRing.GetNode(int(s.serverId))
	if found && mainShard.GetStoreResource().GetAdminAddress() != selfAdminAddress {
		if err := s.doFollow(ctx, int(s.serverId)); err != nil {
			log.Printf("stop following %s %s : %v", mainShard.GetStoreResource().GetAdminAddress(), s, err)
		}
	}

}

func (s *shard) doFollow(ctx context.Context, serverId int) error {

	return s.clusterRing.WithConnection(serverId, func(node topology.Node, grpcConnection *grpc.ClientConn) error {
		return s.followChanges(ctx, node, grpcConnection)
	})

}

func (s *shard) peerShards() []topology.ClusterShard {
	return topology.PeerShards(int(s.serverId), int(s.id), s.clusterRing.ExpectedSize(), s.clusterRing.ReplicationFactor())
}
