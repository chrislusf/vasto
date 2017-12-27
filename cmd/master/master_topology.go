package master

import (
	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/topology"
	"sync"
)

/*
keyspace:
    a logical namespace consists of a logical dataset
data_center:
    a physical location with a set of servers
cluster:
    a set of physical servers in a specific data center, assigned to a keyspace
store:
    a physical server in a specific data center, possibly already assigned to a keyspace
shard:
    a partition of data set of a keyspace
*/

type keyspace_name string
type data_center_name string
type server_address string

type dataCenter struct {
	name    data_center_name
	servers map[server_address]*pb.StoreResource
	sync.RWMutex
}

type keyspace struct {
	sync.RWMutex
	name     keyspace_name
	clusters map[data_center_name]*topology.ClusterRing
}

type dataCenters struct {
	sync.RWMutex
	dataCenters map[data_center_name]*dataCenter
}

type keyspaces struct {
	sync.RWMutex
	keyspaces map[keyspace_name]*keyspace
}

type masterTopology struct {
	keyspaces   *keyspaces
	dataCenters *dataCenters
}

func newMasterTopology() *masterTopology {
	return &masterTopology{
		keyspaces: &keyspaces{
			keyspaces: make(map[keyspace_name]*keyspace),
		},
		dataCenters: &dataCenters{
			dataCenters: make(map[data_center_name]*dataCenter),
		},
	}
}

func (ks *keyspaces) getOrCreateKeyspace(keyspaceName string) *keyspace {
	ks.RLock()
	k, hasData := ks.keyspaces[keyspace_name(keyspaceName)]
	ks.RUnlock()
	if !hasData {
		k = &keyspace{
			name:     keyspace_name(keyspaceName),
			clusters: make(map[data_center_name]*topology.ClusterRing),
		}
		ks.Lock()
		ks.keyspaces[k.name] = k
		ks.Unlock()
	}
	return k
}

func (ks *keyspaces) getKeyspace(keyspaceName string) (k *keyspace, found bool) {
	ks.RLock()
	k, found = ks.keyspaces[keyspace_name(keyspaceName)]
	ks.RUnlock()
	return
}

func (ks *keyspaces) removeKeyspace(keyspaceName string) {
	ks.Lock()
	delete(ks.keyspaces, keyspace_name(keyspaceName))
	ks.Unlock()
}

func (k *keyspace) getCluster(dataCenterName string) (cluster *topology.ClusterRing, found bool) {
	k.RLock()
	cluster, found = k.clusters[data_center_name(dataCenterName)]
	k.RUnlock()
	return
}

func (k *keyspace) removeCluster(dataCenterName string) {
	k.Lock()
	delete(k.clusters, data_center_name(dataCenterName))
	k.Unlock()
}

func (k *keyspace) doGetOrCreateCluster(dataCenterName string, clusterSize int, replicationFactor int) (
	cluster *topology.ClusterRing, isNew bool) {
	cluster, found := k.getCluster(dataCenterName)
	if !found {
		cluster = topology.NewHashRing(string(k.name), dataCenterName, clusterSize, replicationFactor)
		k.Lock()
		k.clusters[data_center_name(dataCenterName)] = cluster
		k.Unlock()
		isNew = true
	}

	return
}

func (k *keyspace) getOrCreateCluster(dataCenterName string, clusterSize int, replicationFactor int) (*topology.ClusterRing) {
	cluster, _ := k.doGetOrCreateCluster(dataCenterName, clusterSize, replicationFactor)
	cluster.SetExpectedSize(clusterSize)
	cluster.SetReplicationFactor(replicationFactor)

	return cluster
}

func (dcs *dataCenters) getOrCreateDataCenter(dataCenterName string) *dataCenter {
	dcs.RLock()
	dc, hasData := dcs.dataCenters[data_center_name(dataCenterName)]
	dcs.RUnlock()
	if !hasData {
		dc = &dataCenter{
			name:    data_center_name(dataCenterName),
			servers: make(map[server_address]*pb.StoreResource),
		}
		dcs.Lock()
		dcs.dataCenters[dc.name] = dc
		dcs.Unlock()
	}
	return dc
}

func (dcs *dataCenters) getDataCenter(dataCenterName string) (dc *dataCenter, found bool) {
	dcs.RLock()
	dc, found = dcs.dataCenters[data_center_name(dataCenterName)]
	dcs.RUnlock()
	return
}

func (dc *dataCenter) upsertServer(storeResource *pb.StoreResource) (existing *pb.StoreResource, hasData bool) {
	dc.RLock()
	existing, hasData = dc.servers[server_address(storeResource.Address)]
	dc.RUnlock()
	if !hasData {
		dc.Lock()
		dc.servers[server_address(storeResource.Address)] = storeResource
		dc.Unlock()
	}
	return
}

func (dcs *dataCenters) deleteServer(dc *dataCenter, storeResource *pb.StoreResource) (
	existing *pb.StoreResource, hasData bool) {
	existing, hasData = dc.doDeleteServer(storeResource)
	if len(dc.servers) == 0 {
		dcs.Lock()
		delete(dcs.dataCenters, dc.name)
		dcs.Unlock()
	}
	return
}

func (dc *dataCenter) doDeleteServer(storeResource *pb.StoreResource) (existing *pb.StoreResource, hasData bool) {
	dc.RLock()
	existing, hasData = dc.servers[server_address(storeResource.Address)]
	dc.RUnlock()
	if hasData {
		dc.Lock()
		delete(dc.servers, server_address(storeResource.Address))
		dc.Unlock()
	}
	return
}
