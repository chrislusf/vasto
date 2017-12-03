package store

import (
	"time"

	"github.com/chrislusf/vasto/topology"
	"github.com/chrislusf/vasto/util"
	"google.golang.org/grpc"
)

// follow keep trying all peers in the cluster and keep retrying to follow the peers
func (n *node) follow() {

	for _, serverId := range n.findPeerServerIds() {
		sid := serverId
		go util.RetryForever(func() error {
			return n.doFollow(sid)
		}, 2*time.Second)
	}

}

func (n *node) doFollow(serverId int) error {

	return n.clusterRing.WithConnection(serverId, func(node topology.Node, grpcConnection *grpc.ClientConn) error {
		return n.followChanges(node, grpcConnection)
	})

}
