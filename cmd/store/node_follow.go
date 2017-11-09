package store

import (
	"fmt"

	"github.com/chrislusf/vasto/util"
	"google.golang.org/grpc"
	"log"
	"time"
)

func (n *node) follow() {

	log.Printf("starts following node %d ...", n.id)

	util.RetryForever(func() error {
		return n.doFollow()
	}, 2*time.Second)
}

func (n *node) doFollow() error {
	node, _, ok := n.clusterListener.GetNode(n.id)

	if !ok {
		return fmt.Errorf("node %d not found", n.id)
	}

	if node == nil {
		return fmt.Errorf("node %d is missing", n.id)
	}

	log.Printf("connecting to node %d at %s", n.id, node.GetAdminAddress())

	grpcConnection, err := grpc.Dial(node.GetAdminAddress(), grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("fail to dial: %v", err)
	}
	defer grpcConnection.Close()

	log.Printf("connected  to node %d at %s", n.id, node.GetAdminAddress())

	return n.followChanges(grpcConnection)

}

func (n *node) maybeInitialize(grpcConnection *grpc.ClientConn) error {
	return nil
}
