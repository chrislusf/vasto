package store

import (
	"time"

	"github.com/chrislusf/vasto/topology"
	"github.com/chrislusf/vasto/util"
	"google.golang.org/grpc"
	"fmt"
)

// follow keep trying all peers in the cluster and keep retrying to follow the peers
func (s *shard) follow() {

	for _, serverId := range s.findPeerServerIds() {
		sid := serverId
		go util.RetryForever(fmt.Sprintf("server %d shard %d to server %d", s.serverId, s.id, sid), func() error {
			return s.doFollow(sid)
		}, 2*time.Second)
	}

}

func (s *shard) doFollow(serverId int) error {

	return s.clusterRing.WithConnection(serverId, func(node topology.Node, grpcConnection *grpc.ClientConn) error {
		return s.followChanges(node, grpcConnection)
	})

}
