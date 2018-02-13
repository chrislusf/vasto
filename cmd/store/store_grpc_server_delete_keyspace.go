package store

import (
	"fmt"
	"github.com/chrislusf/vasto/pb"
	"golang.org/x/net/context"
	"os"
	"github.com/golang/glog"
)

// DeleteKeyspace
// 1. if the shard is already created, do nothing
func (ss *storeServer) DeleteKeyspace(ctx context.Context, request *pb.DeleteKeyspaceRequest) (*pb.DeleteKeyspaceResponse, error) {

	glog.V(1).Infof("delete keyspace %v", request)
	err := ss.deleteShards(request.Keyspace, true)
	if err != nil {
		glog.Errorf("delete keyspace %s: %v", request.Keyspace, err)
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
		ss.shutdownShard(shard)
	}

	// remove all meta info and in-memory objects
	dir := fmt.Sprintf("%s/%s", *ss.option.Dir, keyspace)
	os.RemoveAll(dir)
	ss.keyspaceShards.deleteKeyspace(keyspace)
	ss.deleteServerStatusInCluster(keyspace)
	ss.clusterListener.RemoveKeyspace(keyspace)

	return nil
}

func (ss *storeServer) shutdownShard(shard *shard) {
	ss.UnregisterPeriodicTask(shard)
	shard.clusterListener.UnregisterShardEventProcessor(shard)
	shard.shutdownNode()
	shard.db.Close()
	shard.db.Destroy()
	ss.keyspaceShards.removeShard(shard)
}
