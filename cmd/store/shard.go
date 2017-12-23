package store

import (
	"fmt"
	"github.com/chrislusf/vasto/storage/binlog"
	"github.com/chrislusf/vasto/storage/rocks"
	"github.com/chrislusf/vasto/topology"
	"github.com/chrislusf/vasto/topology/cluster_listener"
	"log"
	"context"
	"sync"
)

type shard_id int
type server_id int

type shard struct {
	keyspace           string
	id                 shard_id
	serverId           server_id
	db                 *rocks.Rocks
	lm                 *binlog.LogManager
	clusterRing        *topology.ClusterRing
	clusterListener    *cluster_listener.ClusterListener
	nodeFinishChan     chan bool
	cancelFunc         context.CancelFunc
	isShutdown         bool
	followProgress     map[progressKey]progressValue
	followProgressLock sync.Mutex
}

func (s *shard) String() string {
	return fmt.Sprintf("%s.%d.%d", s.keyspace, s.serverId, s.id)
}

func newShard(keyspaceName, dir string, serverId, nodeId int, cluster *topology.ClusterRing,
	clusterListener *cluster_listener.ClusterListener,
	replicationFactor int, logFileSizeMb int, logFileCount int) (context.Context, *shard) {

	ctx, cancelFunc := context.WithCancel(context.Background())

	s := &shard{
		keyspace:        keyspaceName,
		id:              shard_id(nodeId),
		serverId:        server_id(serverId),
		db:              rocks.New(dir),
		clusterRing:     cluster,
		clusterListener: clusterListener,
		nodeFinishChan:  make(chan bool),
		cancelFunc:      cancelFunc,
		followProgress:  make(map[progressKey]progressValue),
	}
	if logFileSizeMb > 0 {
		s.lm = binlog.NewLogManager(dir, nodeId, int64(logFileSizeMb*1024*1024), logFileCount)
		s.lm.Initialze()
	}

	return ctx, s
}

func (s *shard) startWithBootstrapAndFollow(ctx context.Context, selfAdminAddress string, mayBootstrap bool) {

	if s.clusterRing != nil && mayBootstrap {
		err := s.bootstrap(ctx)
		if err != nil {
			log.Printf("bootstrap: %v", err)
		}
	}

	s.follow(ctx, selfAdminAddress)

	s.clusterListener.RegisterShardEventProcessor(s)

}

func (s *shard) shutdownNode() {

	s.isShutdown = true

	s.cancelFunc()

	s.clusterListener.RemoveKeyspace(s.keyspace)

	s.clusterListener.UnregisterShardEventProcessor(s)

	close(s.nodeFinishChan)

	if s.lm != nil {
		s.lm.Shutdown()
	}

}

func (s *shard) setCompactionFilterClusterSize(clusterSize int) {

	s.db.SetCompactionForShard(int(s.id), clusterSize)

}
