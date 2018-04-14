package store

import (
	"fmt"

	"github.com/chrislusf/glog"
	"github.com/chrislusf/vasto/pb"
	"golang.org/x/net/context"
)

// CompactKeyspace
// 1. if the shard is already created, do nothing
func (ss *storeServer) CompactKeyspace(ctx context.Context, request *pb.CompactKeyspaceRequest) (*pb.CompactKeyspaceResponse, error) {

	glog.V(1).Infof("compact keyspace %v", request)
	err := ss.compactShards(request.Keyspace)
	if err != nil {
		glog.Errorf("compact keyspace %s: %v", request.Keyspace, err)
		return &pb.CompactKeyspaceResponse{
			Error: err.Error(),
		}, nil
	}

	return &pb.CompactKeyspaceResponse{
		Error: "",
	}, nil

}

func (ss *storeServer) compactShards(keyspace string) (err error) {

	shards, found := ss.keyspaceShards.getShards(keyspace)
	if !found {
		return fmt.Errorf("unexpected shards not found for %s", keyspace)
	}
	for _, shard := range shards {
		shard.db.Compact()
	}

	return nil
}
