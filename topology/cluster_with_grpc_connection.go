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

	return doWithConnect(node, serverId, fn)
}

type PrimaryShards []*pb.ClusterNode

func (nodes PrimaryShards) WithConnection(serverId int, fn func(*pb.ClusterNode, *grpc.ClientConn) error) error {

	if serverId < 0 || serverId >= len(nodes) {
		return fmt.Errorf("server %d not found", serverId)
	}

	node := nodes[serverId]

	return doWithConnect(node, serverId, fn)

}

func doWithConnect(node *pb.ClusterNode, serverId int, fn func(*pb.ClusterNode, *grpc.ClientConn) error) error {

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
