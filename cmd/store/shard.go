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

	if len(existingPrimaryShards) == 0 {
		for i := 0; i < s.cluster.ExpectedSize(); i++ {
			if n, _, ok := s.cluster.GetNode(i); ok {
				existingPrimaryShards = append(existingPrimaryShards, n)
			}
		}
	}

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
		log.Printf("start topo bootstrap %s, existing servers: %v", s.String(), existingPrimaryShards)
		if err := s.topoChangeBootstrap(s.ctx, bootstrapOption, existingPrimaryShards); err != nil {
			log.Printf("topo bootstrap %s: %v", s.String(), err)
			return fmt.Errorf("topo bootstrap %s: %v", s.String(), err)
		}
		log.Printf("finished topo bootstrap %s", s.String())
	}

	// add normal follow
	// TODO if changed cluster size multiple times, for example, keep increasing cluster size, the same follow may happen again!
	log.Printf("%s normal follow %+v, cluster %d replica %d", s.String(), s.peerShards(), s.cluster.ExpectedSize(), s.cluster.ReplicationFactor())
	for _, peer := range s.peerShards() {
		serverId, shardId := peer.ServerId, peer.ShardId
		go util.RetryUntil(s.ctx, fmt.Sprintf("shard %s follow %d.%d", s.String(), serverId, shardId), func() bool {
			clusterSize, replicationFactor := s.cluster.ExpectedSize(), s.cluster.ReplicationFactor()
			return topology.IsShardInLocal(shardId, serverId, clusterSize, replicationFactor)
		}, func() error {
			return s.cluster.WithConnection(
				fmt.Sprintf("%s follow %d.%d", s.String(), serverId, shardId),
				serverId,
				func(node *pb.ClusterNode, grpcConnection *grpc.ClientConn) error {
					return s.followChanges(s.ctx, node, grpcConnection, shardId, bootstrapOption.ToClusterSize)
				},
			)
		}, 2*time.Second)
	}

	oneTimeFollowCtx, oneTimeFollowCancelFunc := context.WithCancel(context.Background())

	// add one time follow during transitional period, there are no retries, assuming the source shards are already up
	log.Printf("%s one-time follow %+v, cluster %v", s.String(), bootstrapOption.TransitionalFollowSource, s.cluster.String())
	for _, shard := range bootstrapOption.TransitionalFollowSource {
		go func(shard topology.ClusterShard, existingPrimaryShards []*pb.ClusterNode) {
			log.Printf("%s one-time follow2 %+v, existing servers: %v", s.String(), shard, existingPrimaryShards)
			topology.PrimaryShards(existingPrimaryShards).WithConnection(
				fmt.Sprintf("%s one-time follow %d.%d", s.String(), shard.ServerId, shard.ShardId),
				shard.ServerId,
				func(node *pb.ClusterNode, grpcConnection *grpc.ClientConn) error {
					return s.followChanges(oneTimeFollowCtx, node, grpcConnection, shard.ShardId, bootstrapOption.ToClusterSize)
				},
			)
		}(shard, existingPrimaryShards)
	}
	s.oneTimeFollowCancel = func() {
		log.Printf("cancelling shard %v one time followings", s.String())
		oneTimeFollowCancelFunc()
	}

	s.clusterListener.RegisterShardEventProcessor(s)

	return nil

}
