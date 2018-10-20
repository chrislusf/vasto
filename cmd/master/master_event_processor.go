package master

import (
	"github.com/chrislusf/glog"
	"sync"
)

type clientsStat struct {
	sync.Mutex
	keyspaceClientCount map[keyspaceName]int
}

func newClientsStat() *clientsStat {
	return &clientsStat{
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

func (c *clientsStat) addClient(keyspace keyspaceName, server serverAddress) {
	c.Lock()
	defer c.Unlock()

	if x, ok := c.keyspaceClientCount[keyspace]; ok {
		c.keyspaceClientCount[keyspace] = x + 1
	} else {
		c.keyspaceClientCount[keyspace] = 1
	}

}

func (c *clientsStat) removeClient(keyspace keyspaceName, server serverAddress) {
	c.Lock()
	defer c.Unlock()

	if x, ok := c.keyspaceClientCount[keyspace]; ok {
		if x-1 <= 0 {
			delete(c.keyspaceClientCount, keyspace)
		} else {
			c.keyspaceClientCount[keyspace] = x - 1
		}
	}

}

func (ms *masterServer) OnClientConnectEvent(keyspace keyspaceName, clientAddress serverAddress, clientName string) {
	glog.V(1).Infof("[master] + client %v from %v keyspace(%v)", clientName, clientAddress, keyspace)
	ms.clientsStat.addClient(keyspace, clientAddress)
}

func (ms *masterServer) OnClientDisconnectEvent(keyspace keyspaceName, clientAddress serverAddress, clientName string) {
	glog.V(1).Infof("[master] - client %v from %v keyspace(%v)", clientName, clientAddress, keyspace)
	ms.clientsStat.removeClient(keyspace, clientAddress)
}
