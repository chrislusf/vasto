package store

import (
	"time"

	"github.com/chrislusf/vasto/topology"
	"github.com/chrislusf/vasto/util"
	"google.golang.org/grpc"
)

func (n *node) follow() {

	// log.Printf("node %d starts following ...", n.id)
	for _, serverId := range n.findPeerServerIds() {
		go util.RetryForever(func() error {
			sid := serverId
			return n.doFollow(sid)
		}, 2*time.Second)
	}

}

func (n *node) doFollow(serverId int) error {

	return n.withConnection(serverId, func(node topology.Node, grpcConnection *grpc.ClientConn) error {
		return n.followChanges(node, grpcConnection)
	})

}

func (n *node) maybeInitialize(grpcConnection *grpc.ClientConn) error {
	return nil
}
