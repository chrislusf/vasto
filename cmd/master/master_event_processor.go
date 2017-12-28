package master

import (
	"log"
	"sync"
	"fmt"
)

type clientsStat struct {
	sync.Mutex
	clusterClientCount  map[string]int
	dcClientCount       map[data_center_name]int
	keyspaceClientCount map[keyspace_name]int
}

func newClientsStat() *clientsStat {
	return &clientsStat{
		clusterClientCount:  make(map[string]int),
		dcClientCount:       make(map[data_center_name]int),
		keyspaceClientCount: make(map[keyspace_name]int),
	}
}

func (c *clientsStat) getKeyspaceClientCount(keyspace keyspace_name) int {
	c.Lock()
	defer c.Unlock()

	if x, ok := c.keyspaceClientCount[keyspace]; ok {
		return x
	}
	return 0
}

func (c *clientsStat) getDataCenterClientCount(dataCenter data_center_name) int {
	c.Lock()
	defer c.Unlock()

	if x, ok := c.dcClientCount[dataCenter]; ok {
		return x
	}
	return 0
}

func (c *clientsStat) getClusterClientCount(keyspace keyspace_name, dataCenter data_center_name) int {
	clusterKey := fmt.Sprintf("%s:%s", keyspace, dataCenter)
	c.Lock()
	defer c.Unlock()

	if x, ok := c.clusterClientCount[clusterKey]; ok {
		return x
	}
	return 0
}

func (c *clientsStat) addClient(keyspace keyspace_name, dataCenter data_center_name, server server_address) {
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

func (c *clientsStat) removeClient(keyspace keyspace_name, dataCenter data_center_name, server server_address) {
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

func (ms *masterServer) OnClientConnectEvent(dc data_center_name, keyspace keyspace_name, clientAddress server_address) {
	log.Printf("+ client %v keyspace(%v) datacenter(%v)", clientAddress, keyspace, dc)
	ms.clientsStat.addClient(keyspace, dc, clientAddress)
}

func (ms *masterServer) OnClientDisconnectEvent(dc data_center_name, keyspace keyspace_name, clientAddress server_address) {
	log.Printf("- client %v keyspace(%v) datacenter(%v)", clientAddress, keyspace, dc)
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
