package master

import (
	"fmt"
	"github.com/chrislusf/vasto/pb"
	"context"
	"github.com/chrislusf/vasto/topology"
)

func (ms *masterServer) ResizeCluster(ctx context.Context, req *pb.ResizeRequest) (resp *pb.ResizeResponse, err error) {

	resp = &pb.ResizeResponse{}

	keyspace, found := ms.topo.keyspaces.getKeyspace(req.Keyspace)
	if !found {
		resp.Error = fmt.Sprintf("no keyspace %v found", req.Keyspace)
		return
	}

	cluster, found := keyspace.getCluster(req.DataCenter)
	if !found {
		resp.Error = fmt.Sprintf("no datacenter %v found", req.DataCenter)
		return
	}

	dc, found := ms.topo.dataCenters.getDataCenter(req.DataCenter)
	if !found {
		resp.Error = fmt.Sprintf("no datacenter %v found", req.DataCenter)
		return
	}

	if cluster.GetNextClusterRing() != nil {
		resp.Error = fmt.Sprintf("cluster %s %s is resizing %d => %d in progress ...",
			req.Keyspace, req.DataCenter, cluster.CurrentSize(), cluster.GetNextClusterRing().ExpectedSize())
		return
	}

	if cluster.ExpectedSize() == int(req.GetClusterSize()) {
		resp.Error = fmt.Sprintf("cluster %s %s is already size %d", req.Keyspace, req.DataCenter, cluster.ExpectedSize())
		return
	}

	var existingServers []*pb.StoreResource
	for _, node := range cluster.GetNodes() {
		existingServers = append(existingServers, &pb.StoreResource{
			Address:      node.GetAddress(),
			AdminAddress: node.GetAdminAddress(),
		})
	}

	if cluster.ExpectedSize() < int(req.GetClusterSize()) {

		// grow the cluster
		// TODO proper quota alocation
		// 1. allocate new servers for the growing cluster
		eachShardSizeGb := uint32(1)
		newServers, allocateErr := allocateServers(cluster, dc, int(req.ClusterSize), float64(eachShardSizeGb))
		if allocateErr != nil {
			resp.Error = fmt.Sprintf("fail to allocate %d servers: %v", req.ClusterSize, allocateErr)
			return
		}

		// 2. create missing shards on existing servers, create new shards on new servers
		servers := append(existingServers, newServers...)
		if err = createShards(ctx, req.Keyspace, req.ClusterSize+uint32(cluster.ExpectedSize()), uint32(cluster.ReplicationFactor()), eachShardSizeGb, servers); err != nil {
			resp.Error = err.Error()
		}

		// 3. tell all servers to bootstrap the new shards

	} else {
		// shrink the cluster
	}

	return
}

// TODO add tags for filtering
func allocateServers(cluster *topology.ClusterRing, dc *dataCenter, serverCount int, eachShardSizeGb float64) ([]*pb.StoreResource, error) {
	servers, err := dc.allocateServers(serverCount, eachShardSizeGb,
		func(resource *pb.StoreResource) bool {

			for _, node := range cluster.GetNodes() {
				if node.GetAddress() == resource.GetAddress() {
					return false
				}
			}

			return true
		})

	return servers, err
}
