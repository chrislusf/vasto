package master

import (
	"io"

	"fmt"
	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/topology"
	"log"
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
	log.Printf("+ store datacenter(%s) %v", storeResource.DataCenter, storeResource.Address)

	dc := ms.topo.dataCenters.getOrCreateDataCenter(storeResource.DataCenter)

	if existing, hasData := dc.upsertServer(storeResource); hasData {
		return fmt.Errorf("duplicate with existing resource %v", existing)
	}
	defer ms.topo.dataCenters.deleteServer(dc, storeResource)

	seenShardsOnThisServer := make(map[string]*pb.ShardInfo)
	defer ms.unRegisterShards(seenShardsOnThisServer, storeResource)

	var e error
	for {
		beat, e := stream.Recv()
		if e != nil {
			break
		}
		if err := ms.processShardInfo(seenShardsOnThisServer, storeResource, beat.ShardInfo); err != nil {
			log.Printf("process shard status %v: %v", beat.ShardInfo, err)
			log.Printf("- store datacenter(%s) %v: %v", storeResource.DataCenter, storeResource.Address, e)
			return err
		}
	}
	log.Printf("- store datacenter(%s) %v: %v", storeResource.DataCenter, storeResource.Address, e)

	return nil
}

func (ms *masterServer) notifyUpdate(ShardInfo *pb.ShardInfo, storeResource *pb.StoreResource, isDelete bool) error {
	return ms.clientChans.notifyStoreResourceUpdate(
		keyspace_name(ShardInfo.KeyspaceName),
		data_center_name(storeResource.DataCenter),
		[]*pb.ClusterNode{
			&pb.ClusterNode{
				StoreResource: storeResource,
				ShardInfo:   ShardInfo,
			},
		},
		isDelete,
	)

}

func (ms *masterServer) processShardInfo(seenShardsOnThisServer map[string]*pb.ShardInfo,
	storeResource *pb.StoreResource, ShardInfo *pb.ShardInfo) error {
	keyspace := ms.topo.keyspaces.getOrCreateKeyspace(ShardInfo.KeyspaceName)
	cluster := keyspace.getOrCreateCluster(storeResource, int(ShardInfo.ClusterSize), int(ShardInfo.ReplicationFactor))

	node, _, found := cluster.GetNode(int(ShardInfo.NodeId))
	if ShardInfo.Status == pb.ShardInfo_DELETED && !found {
		return nil
	}
	if !found {
		node = topology.NewNodeFromStore(storeResource, ShardInfo.NodeId)
		cluster.Add(node)
	}
	if ShardInfo.Status == pb.ShardInfo_DELETED {
		node.RemoveShardInfo(ShardInfo)
		ms.notifyUpdate(ShardInfo, storeResource, true)
		delete(seenShardsOnThisServer, ShardInfo.IdentifierOnThisServer())
		log.Printf("- dc %s keyspace %s node %d shard %d %s cluster %s", storeResource.DataCenter,
			ShardInfo.KeyspaceName, node.GetId(), ShardInfo.ShardId, node.GetAddress(), cluster)
	} else {
		oldShardInfo := node.SetShardInfo(ShardInfo)
		ms.notifyUpdate(ShardInfo, storeResource, false)
		seenShardsOnThisServer[ShardInfo.IdentifierOnThisServer()] = ShardInfo
		if oldShardInfo == nil {
			log.Printf("+ dc %s keyspace %s node %d shard %d %s cluster %s", storeResource.DataCenter,
				ShardInfo.KeyspaceName, node.GetId(), ShardInfo.ShardId, node.GetAddress(), cluster)
		} else if oldShardInfo.Status != ShardInfo.Status {
			log.Printf("* dc %s keyspace %s node %d shard %d %s cluster %s status:%s=>%s", storeResource.DataCenter,
				ShardInfo.KeyspaceName, node.GetId(), ShardInfo.ShardId, node.GetAddress(), cluster,
				oldShardInfo.Status, ShardInfo.Status)
		}
	}

	return nil
}

func (ms *masterServer) unRegisterShards(seenShardsOnThisServer map[string]*pb.ShardInfo, storeResource *pb.StoreResource) {
	for _, ShardInfo := range seenShardsOnThisServer {
		keyspace := ms.topo.keyspaces.getOrCreateKeyspace(string(ShardInfo.KeyspaceName))
		if cluster, found := keyspace.getCluster(storeResource.DataCenter); found {
			cluster.Remove(int(ShardInfo.NodeId)) // just remove the whole node
			ms.notifyUpdate(ShardInfo, storeResource, true)
		}
	}
}
