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

	seenShardsOnThisServer := make(map[string]*pb.ShardStatus)
	defer ms.unRegisterShards(seenShardsOnThisServer, storeResource)

	var e error
	for {
		beat, e := stream.Recv()
		if e != nil {
			break
		}
		if err := ms.processShardStatus(seenShardsOnThisServer, storeResource, beat.ShardStatus); err != nil {
			log.Printf("process shard status %v: %v", beat.ShardStatus, err)
			log.Printf("- store datacenter(%s) %v: %v", storeResource.DataCenter, storeResource.Address, e)
			return err
		}
	}
	log.Printf("- store datacenter(%s) %v: %v", storeResource.DataCenter, storeResource.Address, e)

	return nil
}

func (ms *masterServer) notifyUpdate(shardStatus *pb.ShardStatus, storeResource *pb.StoreResource, isDelete bool) error {
	return ms.clientChans.notifyStoreResourceUpdate(
		keyspace_name(shardStatus.KeyspaceName),
		data_center_name(storeResource.DataCenter),
		[]*pb.ClusterNode{
			&pb.ClusterNode{
				StoreResource: storeResource,
				ShardStatus:   shardStatus,
			},
		},
		isDelete,
	)

}

func (ms *masterServer) processShardStatus(seenShardsOnThisServer map[string]*pb.ShardStatus,
	storeResource *pb.StoreResource, shardStatus *pb.ShardStatus) error {
	keyspace := ms.topo.keyspaces.getOrCreateKeyspace(shardStatus.KeyspaceName)
	cluster, err := keyspace.getOrCreateCluster(storeResource, int(shardStatus.ClusterSize))
	if err != nil {
		return fmt.Errorf("cluster error: %v", err)
	}

	node, _, found := cluster.GetNode(int(shardStatus.NodeId))
	if shardStatus.Status == pb.ShardStatus_DELETED && !found {
		return nil
	}
	if !found {
		node = topology.NewNodeFromStore(storeResource, shardStatus.NodeId)
		cluster.Add(node)
	}
	if shardStatus.Status == pb.ShardStatus_DELETED {
		node.RemoveShardStatus(shardStatus)
		ms.notifyUpdate(shardStatus, storeResource, true)
		delete(seenShardsOnThisServer, shardStatus.IdentifierOnThisServer())
		log.Printf("- dc %s keyspace %s node %d shard %d %s cluster %s", storeResource.DataCenter,
			shardStatus.KeyspaceName, node.GetId(), shardStatus.ShardId, node.GetAddress(), cluster)
	} else {
		oldShardStatus := node.SetShardStatus(shardStatus)
		ms.notifyUpdate(shardStatus, storeResource, false)
		seenShardsOnThisServer[shardStatus.IdentifierOnThisServer()] = shardStatus
		if oldShardStatus == nil {
			log.Printf("+ dc %s keyspace %s node %d shard %d %s cluster %s", storeResource.DataCenter,
				shardStatus.KeyspaceName, node.GetId(), shardStatus.ShardId, node.GetAddress(), cluster)
		} else if oldShardStatus.Status != shardStatus.Status {
			log.Printf("* dc %s keyspace %s node %d shard %d %s cluster %s status:%s=>%s", storeResource.DataCenter,
				shardStatus.KeyspaceName, node.GetId(), shardStatus.ShardId, node.GetAddress(), cluster,
				oldShardStatus.Status, shardStatus.Status)
		}
	}

	return nil
}

func (ms *masterServer) unRegisterShards(seenShardsOnThisServer map[string]*pb.ShardStatus, storeResource *pb.StoreResource) {
	for _, shardStatus := range seenShardsOnThisServer {
		keyspace := ms.topo.keyspaces.getOrCreateKeyspace(string(shardStatus.KeyspaceName))
		if cluster, found := keyspace.getCluster(storeResource.DataCenter); found {
			cluster.Remove(int(shardStatus.NodeId)) // just remove the whole node
			ms.notifyUpdate(shardStatus, storeResource, true)
		}
	}
}
