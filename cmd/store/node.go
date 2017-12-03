package store

import (
	"fmt"
	"github.com/chrislusf/vasto/storage/binlog"
	"github.com/chrislusf/vasto/storage/rocks"
	"github.com/chrislusf/vasto/topology"
	"github.com/chrislusf/vasto/topology/cluster_listener"
	"log"
	"github.com/chrislusf/vasto/pb"
)

type node struct {
	id                int
	serverId          int
	db                *rocks.Rocks
	lm                *binlog.LogManager
	clusterRing       *topology.ClusterRing
	replicationFactor int
	// just to avoid repeatedly create these variables
	nextSegmentKey []byte
	nextOffsetKey  []byte
}

func (ss *storeServer) startExistingNodes(keyspaceName string, storeStatus *pb.StoreStatusInCluster,
	clusterListener *cluster_listener.ClusterListener) {
	cluster := clusterListener.GetClusterRing(keyspaceName)
	for _, shardStatus := range storeStatus.ShardStatuses {
		dir := fmt.Sprintf("%s/%s/%d", *ss.option.Dir, shardStatus.KeyspaceName, shardStatus.ShardId)
		node := newNode(dir, int(storeStatus.Id), int(shardStatus.ShardId), cluster,
			int(storeStatus.ReplicationFactor), *ss.option.LogFileSizeMb, *ss.option.LogFileCount)
		ss.nodes = append(ss.nodes, node)
		if shardStatus.NodeId != shardStatus.ShardId {
			// FIXME only bootstrap and follow if not main? Wrong here!
			go node.startWithBootstrapAndFollow()
		}
		// register the shard at master
		t := shardStatus
		ss.shardStatusChan <- t
	}
}

func newNode(dir string, serverId, nodeId int, cluster *topology.ClusterRing,
	replicationFactor int, logFileSizeMb int, logFileCount int) *node {
	n := &node{
		id:                nodeId,
		serverId:          serverId,
		db:                rocks.New(dir),
		clusterRing:       cluster,
		replicationFactor: replicationFactor,
	}
	if logFileSizeMb > 0 {
		n.lm = binlog.NewLogManager(dir, nodeId, int64(logFileSizeMb*1024*1024), logFileCount)
		n.lm.Initialze()
	}
	n.nextSegmentKey = []byte(fmt.Sprintf("%d.next.segment", n.id))
	n.nextOffsetKey = []byte(fmt.Sprintf("%d.next.offset", n.id))

	return n
}

func (n *node) startWithBootstrapAndFollow() {
	if n.clusterRing != nil {
		err := n.bootstrap()
		if err != nil {
			log.Printf("bootstrap: %v", err)
		}
	}
	n.follow()
}
