package topology

import (
	"fmt"
	"log"

	"google.golang.org/grpc"
)

func (h *ClusterRing) WithConnection(serverId int, fn func(Node, *grpc.ClientConn) error) error {

	node, _, ok := h.GetNode(serverId)

	if !ok {
		return fmt.Errorf("server %d not found", serverId)
	}

	if node == nil {
		return fmt.Errorf("server %d is missing", serverId)
	}

	// log.Printf("connecting to server %d at %s", serverId, node.GetAdminAddress())

	grpcConnection, err := grpc.Dial(node.GetAdminAddress(), grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("fail to dial %s: %v", node.GetAdminAddress(), err)
	}
	defer grpcConnection.Close()

	log.Printf("node %d connected to server %d at %s", node.GetId(), serverId, node.GetAdminAddress())

	return fn(node, grpcConnection)
}
