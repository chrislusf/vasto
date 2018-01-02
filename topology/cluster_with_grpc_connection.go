package topology

import (
	"fmt"
	"log"

	"google.golang.org/grpc"
	"github.com/chrislusf/vasto/pb"
)

func (cluster *Cluster) WithConnection(shardId int, fn func(*pb.ClusterNode, *grpc.ClientConn) error) error {

	node, _, ok := cluster.GetNode(shardId)

	if !ok {
		return fmt.Errorf("shard %d not found", shardId)
	}

	if node == nil {
		return fmt.Errorf("shard %d is missing", shardId)
	}

	// log.Printf("connecting to server %d at %s", shardId, node.GetAdminAddress())

	grpcConnection, err := grpc.Dial(node.StoreResource.AdminAddress, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("fail to dial %s: %v", node.StoreResource.AdminAddress, err)
	}
	defer grpcConnection.Close()

	log.Printf("connect to shard %s on %s", node.ShardInfo.IdentifierOnThisServer(), node.StoreResource.AdminAddress)

	return fn(node, grpcConnection)
}
