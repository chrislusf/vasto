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

type keyspaceName string
type serverAddress string

type dataCenter struct {
	servers map[serverAddress]*pb.StoreResource
	sync.RWMutex
}

type keyspace struct {
	name    keyspaceName
	cluster *topology.Cluster
}

type keyspaces struct {
	sync.RWMutex
	keyspaces map[keyspaceName]*keyspace
}

type masterTopology struct {
	keyspaces  *keyspaces
	dataCenter *dataCenter
}

func newMasterTopology() *masterTopology {
	return &masterTopology{
		keyspaces: &keyspaces{
			keyspaces: make(map[keyspaceName]*keyspace),
		},
		dataCenter: &dataCenter{
			servers: make(map[serverAddress]*pb.StoreResource),
		},
	}
}

func (ks *keyspaces) getOrCreateKeyspace(ksName string) *keyspace {
	ks.Lock()
	k, hasData := ks.keyspaces[keyspaceName(ksName)]
	if !hasData {
		k = &keyspace{
			name: keyspaceName(ksName),
		}
		ks.keyspaces[k.name] = k
	}
	ks.Unlock()
	return k
}

func (ks *keyspaces) getKeyspace(ksName string) (k *keyspace, found bool) {
	ks.RLock()
	k, found = ks.keyspaces[keyspaceName(ksName)]
	ks.RUnlock()
	return
}

func (ks *keyspaces) removeKeyspace(ksName string) {
	ks.Lock()
	delete(ks.keyspaces, keyspaceName(ksName))
	ks.Unlock()
}

func (k *keyspace) doGetOrCreateCluster(clusterSize int, replicationFactor int) (cluster *topology.Cluster, isNew bool) {

	if k.cluster == nil {
		k.cluster = topology.NewCluster(string(k.name), clusterSize, replicationFactor)
		isNew = true
	}

	cluster = k.cluster

	return
}

func (k *keyspace) getOrCreateCluster(clusterSize int, replicationFactor int) *topology.Cluster {
	cluster, _ := k.doGetOrCreateCluster(clusterSize, replicationFactor)
	cluster.SetExpectedSize(clusterSize)
	cluster.SetReplicationFactor(replicationFactor)

	return cluster
}

func (dc *dataCenter) upsertServer(storeResource *pb.StoreResource) (existing *pb.StoreResource, hasData bool) {
	dc.Lock()
	existing, hasData = dc.servers[serverAddress(storeResource.Address)]
	if !hasData {
		dc.servers[serverAddress(storeResource.Address)] = storeResource
	}
	dc.Unlock()
	return
}

func (dc *dataCenter) deleteServer(storeResource *pb.StoreResource) (existing *pb.StoreResource, hasData bool) {
	dc.Lock()
	existing, hasData = dc.servers[serverAddress(storeResource.Address)]
	if hasData {
		delete(dc.servers, serverAddress(storeResource.Address))
	}
	dc.Unlock()
	return
}
