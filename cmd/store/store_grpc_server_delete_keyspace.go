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
	err := ss.deleteShards(request.Keyspace, true)
	if err != nil {
		log.Printf("delete keyspace %s: %v", request.Keyspace, err)
		return &pb.DeleteKeyspaceResponse{
			Error: err.Error(),
		}, nil
	}

	return &pb.DeleteKeyspaceResponse{
		Error: "",
	}, nil

}

func (ss *storeServer) deleteShards(keyspace string, shouldTellMaster bool) (err error) {

	// notify master of the deleted shards
	if shouldTellMaster {
		localShards, found := ss.getServerStatusInCluster(keyspace)
		if !found {
			return nil
		}
		for _, shardInfo := range localShards.ShardMap {
			ss.sendShardInfoToMaster(shardInfo, pb.ShardInfo_DELETED)
		}
	}

	// physically delete the shards
	shards, found := ss.keyspaceShards.getShards(keyspace)
	if !found {
		return nil
	}
	for _, shard := range shards {
		shard.db.Close()
		shard.db.Destroy()
		shard.shutdownNode()
	}

	// remove all meta info and in-memory objects
	ss.UnregisterPeriodicTask(keyspace)
	dir := fmt.Sprintf("%s/%s", *ss.option.Dir, keyspace)
	os.RemoveAll(dir)
	ss.keyspaceShards.deleteKeyspace(keyspace)
	ss.deleteServerStatusInCluster(keyspace)

	return nil
}
