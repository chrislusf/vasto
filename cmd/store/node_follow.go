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
		println("connecting to node", n.id)
		return n.doFollow()
	}, 2*time.Second)
}

func (n *node) doFollow() error {
	node, ok := n.clusterListener.GetNode(n.id)

	if !ok {
		return fmt.Errorf("node %d not found", n.id)
	}

	if node == nil {
		return fmt.Errorf("node %d is missing", n.id)
	}

	grpcConnection, err := grpc.Dial(node.GetAddress(), grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("fail to dial: %v", err)
	}
	defer grpcConnection.Close()

	return n.followChanges(grpcConnection)

}

func (n *node) maybeInitialize(grpcConnection *grpc.ClientConn) error {
	return nil
}
