package topology

import (
	"fmt"
	"log"

	"google.golang.org/grpc"
	"github.com/chrislusf/vasto/pb"
)

func (cluster *Cluster) WithConnection(serverId int, fn func(*pb.ClusterNode, *grpc.ClientConn) error) error {

	node, _, ok := cluster.GetNode(serverId)

	if !ok {
		log.Printf("cluster misses server %d: %+v", serverId, cluster.String())
		return fmt.Errorf("server %d not found", serverId)
	}

	if node == nil {
		return fmt.Errorf("server %d is missing", serverId)
	}

	// log.Printf("connecting to server %d at %s", serverId, node.GetAdminAddress())

	grpcConnection, err := grpc.Dial(node.StoreResource.AdminAddress, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("fail to dial %s: %v", node.StoreResource.AdminAddress, err)
	}
	defer grpcConnection.Close()

	log.Printf("connect to shard %s on %s", node.ShardInfo.IdentifierOnThisServer(), node.StoreResource.AdminAddress)

	return fn(node, grpcConnection)
}
