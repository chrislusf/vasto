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
	"github.com/chrislusf/vasto/util"
	"time"
	"google.golang.org/grpc"
	"github.com/chrislusf/vasto/pb"
)

type shard_id int
type server_id int

type shard struct {
	keyspace            string
	id                  shard_id
	serverId            server_id
	db                  *rocks.Rocks
	lm                  *binlog.LogManager
	cluster             *topology.Cluster
	clusterListener     *cluster_listener.ClusterListener
	nodeFinishChan      chan bool
	cancelFunc          context.CancelFunc
	isShutdown          bool
	followProgress      map[progressKey]progressValue
	followProgressLock  sync.Mutex
	ctx                 context.Context
	oneTimeFollowCancel context.CancelFunc
	hasBackfilled       bool // whether addSst() has been called on this db
}

func (s *shard) String() string {
	return fmt.Sprintf("%s.%d.%d", s.keyspace, s.serverId, s.id)
}

func newShard(keyspaceName, dir string, serverId, nodeId int, cluster *topology.Cluster,
	clusterListener *cluster_listener.ClusterListener,
	replicationFactor int, logFileSizeMb int, logFileCount int) (*shard) {

	ctx, cancelFunc := context.WithCancel(context.Background())

	log.Printf("open %s.%d.%d in %s", keyspaceName, serverId, nodeId, dir)

	s := &shard{
		keyspace:        keyspaceName,
		id:              shard_id(nodeId),
		serverId:        server_id(serverId),
		db:              rocks.New(dir),
		cluster:         cluster,
		clusterListener: clusterListener,
		nodeFinishChan:  make(chan bool),
		cancelFunc: func() {
			log.Printf("cancelling shard %d.%d", serverId, nodeId)
			cancelFunc()
		},
		followProgress: make(map[progressKey]progressValue),
		ctx:            ctx,
	}
	if logFileSizeMb > 0 {
		s.lm = binlog.NewLogManager(dir, nodeId, int64(logFileSizeMb*1024*1024), logFileCount)
		s.lm.Initialze()
	}

	return s
}

func (s *shard) shutdownNode() {

	log.Printf("shutdownNode: %+v", s)

	s.isShutdown = true

	s.cancelFunc()

	close(s.nodeFinishChan)

	if s.lm != nil {
		s.lm.Shutdown()
	}

}

func (s *shard) setCompactionFilterClusterSize(clusterSize int) {

	s.db.SetCompactionForShard(int(s.id), clusterSize)

}

func (s *shard) startWithBootstrapPlan(bootstrapOption *topology.BootstrapPlan, selfAdminAddress string, existingPrimaryShards []*pb.ClusterNode) error {

	// bootstrap the data
	if bootstrapOption.IsNormalStart {
		if s.cluster != nil && bootstrapOption.IsNormalStartBootstrapNeeded {
			err := s.maybeBootstrapAfterRestart(s.ctx)
			if err != nil {
				log.Printf("normal bootstrap %s: %v", s.String(), err)
				return fmt.Errorf("normal bootstrap %s: %v", s.String(), err)
			}
		}
	} else {
		if err := s.topoChangeBootstrap(s.ctx, bootstrapOption, existingPrimaryShards); err != nil {
			log.Printf("topo bootstrap %s: %v", s.String(), err)
			return fmt.Errorf("topo bootstrap %s: %v", s.String(), err)
		}
	}

	// add normal follow
	for _, peer := range s.peerShards() {
		serverId, shardId := peer.ServerId, peer.ShardId
		go util.RetryUntil(s.ctx, fmt.Sprintf("shard %s follow %d.%d", s.String(), serverId, shardId), func() bool {
			clusterSize, replicationFactor := s.cluster.ExpectedSize(), s.cluster.ReplicationFactor()
			return topology.IsShardInLocal(shardId, serverId, clusterSize, replicationFactor)
		}, func() error {
			return s.doFollow(s.ctx, fmt.Sprintf("%s follow %d.%d", s.String(), serverId, shardId),
				serverId, shardId, 0)
		}, 2*time.Second)
	}

	oneTimeFollowCtx, oneTimeFollowCancelFunc := context.WithCancel(context.Background())

	// add one time follow during transitional period, there are no retries, assuming the source shards are already up
	for _, shard := range bootstrapOption.TransitionalFollowSource {
		go func(shard topology.ClusterShard) {
			sourceShard, _, found := s.cluster.GetNode(int(shard.ServerId))
			if found && sourceShard.GetStoreResource().GetAdminAddress() != selfAdminAddress {
				if err := s.doFollow(oneTimeFollowCtx,
					fmt.Sprintf("%s one-time follow %d.%d", s.String(), shard.ServerId, shard.ShardId),
					shard.ServerId, shard.ShardId, bootstrapOption.ToClusterSize); err != nil {
					log.Printf("shard %s stop following %v : %v", s, sourceShard.ShardInfo.IdentifierOnThisServer(), err)
				}
			}
		}(shard)
	}
	s.oneTimeFollowCancel = func() {
		log.Printf("cancelling shard %v one time followings", s.String())
		oneTimeFollowCancelFunc()
	}

	s.clusterListener.RegisterShardEventProcessor(s)

	return nil

}

func (s *shard) doFollow(ctx context.Context, name string, serverId int, sourceShardId int, targetClusterSize int) error {

	return s.cluster.WithConnection(name, serverId, func(node *pb.ClusterNode, grpcConnection *grpc.ClientConn) error {
		return s.followChanges(ctx, node, grpcConnection, sourceShardId, targetClusterSize)
	})

}
