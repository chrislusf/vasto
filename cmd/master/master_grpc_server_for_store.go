package master

import (
	"io"

	"fmt"
	"github.com/chrislusf/glog"
	"github.com/chrislusf/vasto/pb"
)

func (ms *masterServer) RegisterStore(stream pb.VastoMaster_RegisterStoreServer) error {
	var storeHeartbeat *pb.StoreHeartbeat
	var err error

	storeHeartbeat, err = stream.Recv()
	if err == io.EOF {
		return nil
	}
	if err != nil {
		return err
	}

	// add server to the data center
	storeResource := storeHeartbeat.StoreResource
	glog.V(1).Infof("[master] + store %v", storeResource.Address)

	dc := ms.topo.dataCenter

	if existing, hasData := dc.upsertServer(storeResource); hasData {
		return fmt.Errorf("duplicate with existing resource %v", existing)
	}
	defer ms.topo.dataCenter.deleteServer(storeResource)

	seenShardsOnThisServer := make(map[string]*pb.ShardInfo)
	defer ms.unRegisterShards(seenShardsOnThisServer, storeResource)

	var e error
	for {
		beat, e := stream.Recv()
		if e != nil {
			break
		}
		if err := ms.processShardInfo(seenShardsOnThisServer, storeResource, beat.ShardInfo); err != nil {
			glog.Errorf("process shard status %v: %v", beat.ShardInfo, err)
			glog.Errorf("[master] - store %v: %v", storeResource.Address, e)
			return err
		}
	}
	glog.V(1).Infof("[master] - store %v: %v", storeResource.Address, e)

	return nil
}

func (ms *masterServer) notifyUpdate(shardInfo *pb.ShardInfo, storeResource *pb.StoreResource) error {
	return ms.clientChans.notifyStoreResourceUpdate(
		keyspaceName(shardInfo.KeyspaceName),
		[]*pb.ClusterNode{
			{
				StoreResource: storeResource,
				ShardInfo:     shardInfo,
			},
		},
		false,
		false,
	)
}

func (ms *masterServer) notifyDeletion(shardInfo *pb.ShardInfo, storeResource *pb.StoreResource) error {
	return ms.clientChans.notifyStoreResourceUpdate(
		keyspaceName(shardInfo.KeyspaceName),
		[]*pb.ClusterNode{
			{
				StoreResource: storeResource,
				ShardInfo:     shardInfo,
			},
		},
		true,
		false,
	)
}

func (ms *masterServer) notifyPromotion(shardInfo *pb.ShardInfo, storeResource *pb.StoreResource) error {
	return ms.clientChans.notifyStoreResourceUpdate(
		keyspaceName(shardInfo.KeyspaceName),
		[]*pb.ClusterNode{
			{
				StoreResource: storeResource,
				ShardInfo:     shardInfo,
			},
		},
		false,
		true,
	)
}

func (ms *masterServer) processShardInfo(seenShardsOnThisServer map[string]*pb.ShardInfo,
	storeResource *pb.StoreResource, shardInfo *pb.ShardInfo) error {
	keyspace := ms.topo.keyspaces.getOrCreateKeyspace(shardInfo.KeyspaceName)
	cluster := keyspace.getOrCreateCluster(int(shardInfo.ClusterSize), int(shardInfo.ReplicationFactor))

	if shardInfo.IsCandidate {
		if cluster.GetNextCluster() == nil {
			cluster.SetNextCluster(int(shardInfo.ClusterSize), int(shardInfo.ReplicationFactor))
		}
		cluster = cluster.GetNextCluster()
	}

	if shardInfo.Status == pb.ShardInfo_DELETED {
		cluster.RemoveShard(storeResource, shardInfo)
		ms.notifyDeletion(shardInfo, storeResource)
		delete(seenShardsOnThisServer, shardInfo.IdentifierOnThisServer())
		glog.V(2).Infof("[master] - %s on %s master cluster %s",
			shardInfo.IdentifierOnThisServer(), storeResource.Address, cluster)
	} else {
		// println("updated shard info:", shardInfo.String(), "store", storeResource.GetAddress())
		oldShardInfo := cluster.SetShard(storeResource, shardInfo)
		ms.notifyUpdate(shardInfo, storeResource)
		seenShardsOnThisServer[shardInfo.IdentifierOnThisServer()] = shardInfo
		if oldShardInfo == nil {
			if shardInfo.IsCandidate {
				glog.V(1).Infof("[master] => %s on %s master cluster %s",
					shardInfo.IdentifierOnThisServer(), storeResource.Address, cluster)
			} else {
				glog.V(1).Infof("[master] + %s on %s master cluster %s",
					shardInfo.IdentifierOnThisServer(), storeResource.Address, cluster)
			}
		} else if oldShardInfo.Status != shardInfo.Status {
			glog.V(1).Infof("[master] * %s on %s master cluster %s status:%s=>%s",
				shardInfo.IdentifierOnThisServer(), storeResource.Address, cluster,
				oldShardInfo.Status, shardInfo.Status)
		}
	}

	return nil
}

func (ms *masterServer) unRegisterShards(seenShardsOnThisServer map[string]*pb.ShardInfo, storeResource *pb.StoreResource) {
	for _, shardInfo := range seenShardsOnThisServer {
		keyspace := ms.topo.keyspaces.getOrCreateKeyspace(string(shardInfo.KeyspaceName))
		cluster := keyspace.cluster
		if cluster != nil {
			if shardInfo.IsCandidate {
				if keyspace.cluster.GetNextCluster() == nil {
					continue
				}
				cluster = cluster.GetNextCluster()
			}
			cluster.RemoveShard(storeResource, shardInfo)
			ms.notifyDeletion(shardInfo, storeResource)
		}
	}
}
