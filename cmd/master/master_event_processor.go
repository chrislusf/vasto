package master

import (
	"fmt"
	"github.com/chrislusf/glog"
	"sync"
)

type clientsStat struct {
	sync.Mutex
	clusterClientCount  map[string]int
	dcClientCount       map[datacenterName]int
	keyspaceClientCount map[keyspaceName]int
}

func newClientsStat() *clientsStat {
	return &clientsStat{
		clusterClientCount:  make(map[string]int),
		dcClientCount:       make(map[datacenterName]int),
		keyspaceClientCount: make(map[keyspaceName]int),
	}
}

func (c *clientsStat) getKeyspaceClientCount(keyspace keyspaceName) int {
	c.Lock()
	defer c.Unlock()

	if x, ok := c.keyspaceClientCount[keyspace]; ok {
		return x
	}
	return 0
}

func (c *clientsStat) getDataCenterClientCount(dataCenter datacenterName) int {
	c.Lock()
	defer c.Unlock()

	if x, ok := c.dcClientCount[dataCenter]; ok {
		return x
	}
	return 0
}

func (c *clientsStat) getClusterClientCount(keyspace keyspaceName, dataCenter datacenterName) int {
	clusterKey := fmt.Sprintf("%s:%s", keyspace, dataCenter)
	c.Lock()
	defer c.Unlock()

	if x, ok := c.clusterClientCount[clusterKey]; ok {
		return x
	}
	return 0
}

func (c *clientsStat) addClient(keyspace keyspaceName, dataCenter datacenterName, server serverAddress) {
	clusterKey := fmt.Sprintf("%s:%s", keyspace, dataCenter)
	c.Lock()
	defer c.Unlock()
	if x, ok := c.clusterClientCount[clusterKey]; ok {
		c.clusterClientCount[clusterKey] = x + 1
	} else {
		c.clusterClientCount[clusterKey] = 1
	}

	if x, ok := c.dcClientCount[dataCenter]; ok {
		c.dcClientCount[dataCenter] = x + 1
	} else {
		c.dcClientCount[dataCenter] = 1
	}

	if x, ok := c.keyspaceClientCount[keyspace]; ok {
		c.keyspaceClientCount[keyspace] = x + 1
	} else {
		c.keyspaceClientCount[keyspace] = 1
	}

}

func (c *clientsStat) removeClient(keyspace keyspaceName, dataCenter datacenterName, server serverAddress) {
	clusterKey := fmt.Sprintf("%s:%s", keyspace, dataCenter)
	c.Lock()
	defer c.Unlock()
	if x, ok := c.clusterClientCount[clusterKey]; ok {
		if x-1 <= 0 {
			delete(c.clusterClientCount, clusterKey)
		} else {
			c.clusterClientCount[clusterKey] = x - 1
		}
	}

	if x, ok := c.dcClientCount[dataCenter]; ok {
		if x-1 <= 0 {
			delete(c.dcClientCount, dataCenter)
		} else {
			c.dcClientCount[dataCenter] = x - 1
		}
	}

	if x, ok := c.keyspaceClientCount[keyspace]; ok {
		if x-1 <= 0 {
			delete(c.keyspaceClientCount, keyspace)
		} else {
			c.keyspaceClientCount[keyspace] = x - 1
		}
	}

}

func (ms *masterServer) OnClientConnectEvent(dc datacenterName, keyspace keyspaceName, clientAddress serverAddress, clientName string) {
	glog.V(1).Infof("[master] + client %v from %v keyspace(%v) datacenter(%v)", clientName, clientAddress, keyspace, dc)
	ms.clientsStat.addClient(keyspace, dc, clientAddress)
}

func (ms *masterServer) OnClientDisconnectEvent(dc datacenterName, keyspace keyspaceName, clientAddress serverAddress, clientName string) {
	glog.V(1).Infof("[master] - client %v from %v keyspace(%v) datacenter(%v)", clientName, clientAddress, keyspace, dc)
	ms.clientsStat.removeClient(keyspace, dc, clientAddress)
	if ms.clientsStat.getClusterClientCount(keyspace, dc) <= 0 {
		if k, found := ms.topo.keyspaces.getKeyspace(string(keyspace)); found {
			k.removeCluster(string(dc))
			if len(k.clusters) == 0 {
				ms.topo.keyspaces.removeKeyspace(string(keyspace))
			}
		}
	}
}
