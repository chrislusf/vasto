package store

import (
	"fmt"
	"github.com/chrislusf/vasto/storage/binlog"
	"github.com/chrislusf/vasto/storage/rocks"
	"github.com/chrislusf/vasto/topology"
	"github.com/chrislusf/vasto/topology/cluster_listener"
	"github.com/chrislusf/vasto/pb"
	"log"
	"context"
)

type node struct {
	keyspace          string
	id                int
	serverId          int
	db                *rocks.Rocks
	lm                *binlog.LogManager
	clusterRing       *topology.ClusterRing
	clusterListener   *cluster_listener.ClusterListener
	replicationFactor int
	nodeFinishChan    chan bool
	ctx               context.Context
	cancelFunc        context.CancelFunc
	// just to avoid repeatedly create these variables
	nextSegmentKey, nextOffsetKey []byte
	prevSegment                   uint32
	prevOffset                    uint64
	nextSegment                   uint32
	nextOffset                    uint64
}

func (n *node) String() string {
	return fmt.Sprintf("%s.%d.%d", n.keyspace, n.serverId, n.id)
}

func (ss *storeServer) startExistingNodes(keyspaceName string, storeStatus *pb.StoreStatusInCluster,
	clusterListener *cluster_listener.ClusterListener) {
	cluster := clusterListener.GetClusterRing(keyspaceName)
	for _, shardStatus := range storeStatus.ShardStatuses {
		dir := fmt.Sprintf("%s/%s/%d", *ss.option.Dir, shardStatus.KeyspaceName, shardStatus.ShardId)
		node := newNode(keyspaceName, dir, int(storeStatus.Id), int(shardStatus.ShardId), cluster, clusterListener,
			int(storeStatus.ReplicationFactor), *ss.option.LogFileSizeMb, *ss.option.LogFileCount)
		ss.keyspaceShards.addShards(keyspaceName, node)
		ss.RegisterPeriodicTask(node)
		go node.startWithBootstrapAndFollow(*ss.option.Bootstrap)

		println("size", node.db.Size())

		// register the shard at master
		t := shardStatus
		ss.shardStatusChan <- t
	}
}

func newNode(keyspaceName, dir string, serverId, nodeId int, cluster *topology.ClusterRing,
	clusterListener *cluster_listener.ClusterListener,
	replicationFactor int, logFileSizeMb int, logFileCount int) *node {

	ctx, cancelFunc := context.WithCancel(context.Background())

	n := &node{
		keyspace:          keyspaceName,
		id:                nodeId,
		serverId:          serverId,
		db:                rocks.New(dir),
		clusterRing:       cluster,
		clusterListener:   clusterListener,
		replicationFactor: replicationFactor,
		nodeFinishChan:    make(chan bool),
		ctx:               ctx,
		cancelFunc:        cancelFunc,
	}
	if logFileSizeMb > 0 {
		n.lm = binlog.NewLogManager(dir, nodeId, int64(logFileSizeMb*1024*1024), logFileCount)
		n.lm.Initialze()
	}
	n.nextSegmentKey = []byte(fmt.Sprintf("%d.next.segment", n.id))
	n.nextOffsetKey = []byte(fmt.Sprintf("%d.next.offset", n.id))

	return n
}

func (n *node) startWithBootstrapAndFollow(mayBootstrap bool) {

	if n.clusterRing != nil && mayBootstrap {
		err := n.bootstrap()
		if err != nil {
			log.Printf("bootstrap: %v", err)
		}
	}

	n.follow()

	n.clusterListener.RegisterShardEventProcessor(n)

}

func (n *node) shutdownNode() {

	n.clusterListener.RemoveKeyspace(n.keyspace)

	n.clusterListener.UnregisterShardEventProcessor(n)

	n.cancelFunc()

	close(n.nodeFinishChan)

}
