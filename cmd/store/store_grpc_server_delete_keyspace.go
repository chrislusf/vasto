package store

import (
	"github.com/chrislusf/vasto/pb"
	"golang.org/x/net/context"
	"log"
	"fmt"
	"os"
)

// DeleteKeyspace
// 1. if the shard is already created, do nothing
func (ss *storeServer) DeleteKeyspace(ctx context.Context, request *pb.DeleteKeyspaceRequest) (*pb.DeleteKeyspaceResponse, error) {

	log.Printf("delete keyspace %v", request)
	err := ss.deleteShards(request.Keyspace)
	if err != nil {
		log.Printf("delete keyspace %s: %v", request.Keyspace, err)
		return &pb.DeleteKeyspaceResponse{
			Error: err.Error(),
		}, nil
	}
	ss.keyspaceShards.deleteKeyspace(request.Keyspace)

	return &pb.DeleteKeyspaceResponse{
		Error: "",
	}, nil

}

func (ss *storeServer) deleteShards(keyspace string) (err error) {

	nodes := ss.keyspaceShards.getShards(keyspace)

	for _, node := range nodes {
		node.db.Close()
		node.db.Destroy()
		node.shutdownNode()
	}

	ss.UnregisterPeriodicTask(keyspace)
	dir := fmt.Sprintf("%s/%s", *ss.option.Dir, keyspace)
	os.RemoveAll(dir)

	return nil
}
