package store

import (
	"time"

	"github.com/chrislusf/vasto/topology"
	"github.com/chrislusf/vasto/util"
	"google.golang.org/grpc"
	"fmt"
	"context"
)

// follow keep trying all peers in the cluster and keep retrying to follow the peers
func (s *shard) follow(ctx context.Context) {

	for _, serverId := range s.findPeerServerIds() {
		sid := serverId
		go util.RetryForever(ctx, fmt.Sprintf("server %d shard %d to server %d", s.serverId, s.id, sid), func() error {
			return s.doFollow(ctx, sid)
		}, 2*time.Second)
	}

}

func (s *shard) doFollow(ctx context.Context, serverId int) error {

	return s.clusterRing.WithConnection(serverId, func(node topology.Node, grpcConnection *grpc.ClientConn) error {
		return s.followChanges(ctx, node, grpcConnection)
	})

}
