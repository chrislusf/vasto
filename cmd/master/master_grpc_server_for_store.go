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
	log.Printf("+ %s %s %v\n", storeResource.DataCenter, storeResource.Network, storeResource.Address)

	dc := ms.topo.dataCenters.getOrCreateDataCenter(storeResource.DataCenter)

	if existing, hasData := dc.upsertServer(storeResource); hasData {
		return fmt.Errorf("duplicate with existing resource %v", existing)
	}
	defer ms.topo.dataCenters.deleteServer(dc, storeResource)

	// add shards on the server to the keyspace
	for keyspaceName, shard := range storeResource.StoreStatusesInCluster {
		keyspace := ms.topo.keyspaces.getOrCreateKeyspace(keyspaceName)
		cluster, err := keyspace.getOrCreateCluster(storeResource, shard)
		if err != nil {
			log.Printf("cluster error: %v", err)
			continue
		}

		node := topology.NewNodeFromStore(storeResource, shard.Id)

		cluster.Add(node)
		ms.notifyUpdate(shard.Id, keyspaceName, storeResource, false)
		defer func(keyspaceName string, shard *pb.StoreStatusInCluster) {
			cluster.Remove(int(shard.Id))
			ms.notifyUpdate(shard.Id, keyspaceName, storeResource, true)
		}(keyspaceName, shard)
	}

	// loop through new shard status updates
	var e error
	for {
		beat, e := stream.Recv()
		if e != nil {
			break
		}

		// FIXME cover all cases
		// loop through old shards, compare them with the new ones
		for keyspaceName, oldShard := range storeResource.StoreStatusesInCluster {
			newStoreStatus, foundNewStoreStatus := beat.StoreResource.StoreStatusesInCluster[keyspaceName]
			if foundNewStoreStatus {
				for shardId, newShardStatus := range newStoreStatus.NodeStatuses {
					if oldShardStatus, foundOldShardStatus := oldShard.NodeStatuses[shardId]; foundOldShardStatus {
						if oldShardStatus.Status != newShardStatus.Status {
							log.Printf("* dc %s server %s keyspace %s shard %d status %v => %v",
								storeResource.DataCenter,
								storeResource.Address,
								keyspaceName,
								shardId, oldShardStatus.Status, newShardStatus.Status)
						}
					} else {
						log.Printf("+ dc %s server %s keyspace %s shard %d status %v",
							storeResource.DataCenter,
							storeResource.Address,
							keyspaceName,
							shardId, newShardStatus.Status)
					}
					oldShard.NodeStatuses[shardId] = newShardStatus
					ms.notifyUpdate(shardId, keyspaceName, storeResource, false)
				}
			}
		}

	}
	log.Printf("- %s %s %v : %v\n", storeHeartbeat.StoreResource.DataCenter,
		storeHeartbeat.StoreResource.Network, storeHeartbeat.StoreResource.Address, e)

	return nil
}

func (ms *masterServer) notifyUpdate(shardId uint32,
	keyspaceName string, storeResource *pb.StoreResource, isDelete bool) error {
	return ms.clientChans.notifyStoreResourceUpdate(
		keyspaceName,
		storeResource.DataCenter,
		[]*pb.ClusterNode{
			&pb.ClusterNode{
				ShardId:      shardId,
				Network:      storeResource.Network,
				Address:      storeResource.Address,
				AdminAddress: storeResource.AdminAddress,
			},
		},
		isDelete,
	)

}
