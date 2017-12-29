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

	if cluster.ExpectedSize() < int(req.GetClusterSize()) {

		// grow the cluster
		// TODO proper quota alocation
		eachShardSizeGb := uint32(1)
		servers, err := allocateServers(cluster, dc, int(req.ClusterSize), float64(eachShardSizeGb))
		if err != nil {
			resp.Error = fmt.Sprintf("fail to allocate %d servers: %v", req.ClusterSize, err)
			return
		}

		if err = createShards(ctx, req.Keyspace, req.ClusterSize+uint32(cluster.ExpectedSize()), uint32(cluster.ReplicationFactor()), eachShardSizeGb, servers, cluster.ExpectedSize()); err != nil {
			resp.Error = err.Error()
		}

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
