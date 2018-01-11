package store

import (
	"fmt"
	"io"
	"log"

	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/topology"
	"github.com/tecbot/gorocksdb"
	"google.golang.org/grpc"
	"context"
	"sync"
	"github.com/chrislusf/vasto/util"
	"github.com/chrislusf/vasto/storage/codec"
)

func (s *shard) peerShards() []topology.ClusterShard {
	return topology.PeerShards(int(s.serverId), int(s.id), s.cluster.ExpectedSize(), s.cluster.ReplicationFactor())
}

/*
bootstrap ensure current shard is bootstrapped and can be synced by binlog tailing.
1. checks whether the binlog offset is behind any other nodes, if not, return
2. delete all local data and binlog offset
3. starts to add changes to sstable
4. get the new offsets
*/
func (s *shard) maybeBootstrapAfterRestart(ctx context.Context) error {

	bestPeerToCopy, isNeeded := s.isBootstrapNeeded(ctx, &topology.BootstrapPlan{
		BootstrapSource: s.peerShards(),
	})

	if !isNeeded {
		// log.Printf("bootstrap shard %d is not needed", s.id)
		return nil
	}

	log.Printf("bootstrap from server %+v ...", bestPeerToCopy)

	return s.cluster.WithConnection(fmt.Sprintf("%s bootstrap after restart", s.String()), bestPeerToCopy.ServerId, func(node *pb.ClusterNode, grpcConnection *grpc.ClientConn) error {
		_, canTailBinlog, err := s.checkBinlogAvailable(ctx, grpcConnection, node)
		if err != nil {
			return err
		}
		if !canTailBinlog {
			return s.doBootstrapCopy(ctx, grpcConnection, node, s.cluster.ExpectedSize(), 0, 0)
		}
		return nil
	})

}

func (s *shard) topoChangeBootstrap(ctx context.Context, bootstrapPlan *topology.BootstrapPlan, existingPrimaryShards []*pb.ClusterNode) error {

	if bootstrapPlan == nil {
		return nil
	}

	if len(existingPrimaryShards) == 0 {
		for i := 0; i < s.cluster.ExpectedSize(); i++ {
			if n, _, ok := s.cluster.GetNode(i); ok {
				existingPrimaryShards = append(existingPrimaryShards, n)
			}
		}
	}

	if bootstrapPlan.PickBestBootstrapSource {

		bestPeerToCopy, isNeeded := s.isBootstrapNeeded(ctx, bootstrapPlan)
		if !isNeeded {
			// log.Printf("bootstrap shard %d is not needed", s.id)
			return nil
		}

		log.Printf("bootstrap from %d.%d ...", bestPeerToCopy.ServerId, bestPeerToCopy.ShardId)

		return topology.PrimaryShards(existingPrimaryShards).WithConnection(fmt.Sprintf("%s bootstrap from one exisitng %d.%d", s.String(), bestPeerToCopy.ServerId, bestPeerToCopy.ShardId),
			bestPeerToCopy.ServerId, func(node *pb.ClusterNode, grpcConnection *grpc.ClientConn) error {
				return s.doBootstrapCopy(ctx, grpcConnection, node, bootstrapPlan.FromClusterSize, bootstrapPlan.ToClusterSize, int(s.id))
			})
	}

	var bootstrapSourceServerIds []int
	var sourceRowChans []chan *pb.KeyValue
	for _, shard := range bootstrapPlan.BootstrapSource {
		bootstrapSourceServerIds = append(bootstrapSourceServerIds, shard.ServerId)
		sourceRowChans = append(sourceRowChans, make(chan *pb.KeyValue, BOOTSTRAP_COPY_BATCH_SIZE))
	}

	return util.Parallel(
		func() error {
			return eachInt(bootstrapSourceServerIds, func(index, serverId int) error {
				sourceChan := sourceRowChans[index]
				defer close(sourceChan)
				return topology.PrimaryShards(existingPrimaryShards).WithConnection(
					fmt.Sprintf("%s bootstrap copy from existing server %d", s.String(), serverId),
					serverId,
					func(node *pb.ClusterNode, grpcConnection *grpc.ClientConn) error {
						return s.doBootstrapCopy2(ctx, grpcConnection, node, bootstrapPlan.FromClusterSize, bootstrapPlan.ToClusterSize, int(s.id), sourceChan)
					},
				)
			})
		},
		func() error {
			if !s.hasBackfilled {
				s.hasBackfilled = true
				log.Printf("bootstrap %v via sst ...", s.String())
				return s.db.AddSstByWriter(fmt.Sprintf("%s bootstrapCopy write", s.String()),
					func(w *gorocksdb.SSTFileWriter) (int, error) {
						var counter int
						err := pb.MergeSorted(sourceRowChans, func(keyValue *pb.KeyValue) error {

							if err := w.Add(keyValue.Key, keyValue.Value); err != nil {
								return fmt.Errorf("add to sst: %v", err)
							}
							counter++
							return nil
						})
						return counter, err
					},
				)
			}

			// slow way to backfill
			log.Printf("bootstrap %v via sorted insert ...", s.String())
			return pb.MergeSorted(sourceRowChans, func(keyValue *pb.KeyValue) error {

				b, err := s.db.Get(keyValue.Key)
				if err != nil || len(b) == 0 {
					return s.db.Put(keyValue.Key, keyValue.Value)
				}

				existingRow := codec.FromBytes(b)
				if existingRow.IsExpired() {
					return s.db.Put(keyValue.Key, keyValue.Value)
				}

				incomingRow := codec.FromBytes(keyValue.Value)
				if existingRow.UpdatedAtNs < incomingRow.UpdatedAtNs {
					return s.db.Put(keyValue.Key, keyValue.Value)
				}

				return nil

			})

		},
	)

}

func (s *shard) checkBinlogAvailable(ctx context.Context, grpcConnection *grpc.ClientConn, node *pb.ClusterNode) (latestSegment uint32, canTailBinlog bool, err error) {

	segment, _, hasProgress, err := s.loadProgress(node.StoreResource.GetAdminAddress(), s.id)

	// println("shard", s.id, "segment", segment, "hasProgress", hasProgress, "err", err)

	if !hasProgress {
		return 0, false, nil
	}

	if err != nil {
		return 0, false, err
	}

	client := pb.NewVastoStoreClient(grpcConnection)

	resp, err := client.CheckBinlog(ctx, &pb.CheckBinlogRequest{
		Keyspace: s.keyspace,
		ShardId:  uint32(s.id),
	})
	if err != nil {
		return 0, false, err
	}

	return resp.LatestSegment, resp.EarliestSegment <= segment, nil

}

func (s *shard) doBootstrapCopy(ctx context.Context, grpcConnection *grpc.ClientConn, node *pb.ClusterNode, clusterSize, targetClusterSize int, targetShardId int) error {

	log.Printf("bootstrap %s from %s %s filter by %d/%d", s.String(), node.StoreResource.Address, node.ShardInfo.IdentifierOnThisServer(), targetShardId, targetClusterSize)

	segment, offset, err := s.writeToSst(ctx, grpcConnection, node.ShardInfo, clusterSize, targetClusterSize, targetShardId)

	if err != nil {
		return fmt.Errorf("writeToSst: %v", err)
	}

	return s.saveProgress(node.StoreResource.GetAdminAddress(), shard_id(targetShardId), segment, offset)

}

func (s *shard) doBootstrapCopy2(ctx context.Context, grpcConnection *grpc.ClientConn, node *pb.ClusterNode, clusterSize, targetClusterSize int, targetShardId int, rowChan chan *pb.KeyValue) (err error) {

	log.Printf("bootstrap2 %s from %s %s filter by %d/%d", s.String(), node.StoreResource.Address, node.ShardInfo.IdentifierOnThisServer(), targetShardId, targetClusterSize)

	client := pb.NewVastoStoreClient(grpcConnection)

	request := &pb.BootstrapCopyRequest{
		Keyspace:          s.keyspace,
		ShardId:           node.ShardInfo.ShardId,
		ClusterSize:       uint32(clusterSize),
		TargetShardId:     uint32(targetShardId),
		TargetClusterSize: uint32(targetClusterSize),
		Origin:            s.String(),
	}

	stream, err := client.BootstrapCopy(ctx, request)
	if err != nil {
		return fmt.Errorf("client.TailBinlog: %v", err)
	}

	var segment uint32
	var offset uint64

	for {

		response, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("bootstrap copy: %v", err)
		}

		for _, keyValue := range response.KeyValues {

			// fmt.Printf("%s add to sst: %v\n", sourceShardInfo.IdentifierOnThisServer(), string(keyValue.Key))
			t := keyValue
			rowChan <- t

		}

		if response.BinlogTailProgress != nil {
			segment = response.BinlogTailProgress.Segment
			offset = response.BinlogTailProgress.Offset
		}

	}

	log.Printf("bootstrap2 %s from %s segment:offset=%d:%d : %v", s.String(), node.ShardInfo.IdentifierOnThisServer(), segment, offset, err)
	s.saveProgress(node.StoreResource.GetAdminAddress(), shard_id(targetShardId), segment, offset)

	return
}

func (s *shard) writeToSst(ctx context.Context, grpcConnection *grpc.ClientConn, sourceShardInfo *pb.ShardInfo, clusterSize, targetClusterSize int, targetShardId int) (segment uint32, offset uint64, err error) {

	client := pb.NewVastoStoreClient(grpcConnection)

	request := &pb.BootstrapCopyRequest{
		Keyspace:          s.keyspace,
		ShardId:           sourceShardInfo.ShardId,
		ClusterSize:       uint32(clusterSize),
		TargetShardId:     uint32(targetShardId),
		TargetClusterSize: uint32(targetClusterSize),
		Origin:            s.String(),
	}

	stream, err := client.BootstrapCopy(ctx, request)
	if err != nil {
		return 0, 0, fmt.Errorf("client.BootstrapCopy: %v", err)
	}

	err = s.db.AddSstByWriter(fmt.Sprintf("bootstrap %s from %s %d/%d", s.String(), sourceShardInfo.IdentifierOnThisServer(), targetShardId, targetClusterSize),

		func(w *gorocksdb.SSTFileWriter) (int, error) {

			var counter int

			for {

				// println("TailBinlog receive from", s.id)

				response, err := stream.Recv()
				if err == io.EOF {
					return counter, nil
				}
				if err != nil {
					return counter, fmt.Errorf("bootstrap copy: %v", err)
				}

				for _, keyValue := range response.KeyValues {

					// fmt.Printf("%s add to sst: %v\n", sourceShardInfo.IdentifierOnThisServer(), string(keyValue.Key))

					err = w.Add(keyValue.Key, keyValue.Value)
					if err != nil {
						return counter, fmt.Errorf("add to sst: %v", err)
					}
					counter++

				}

				if response.BinlogTailProgress != nil {
					segment = response.BinlogTailProgress.Segment
					offset = response.BinlogTailProgress.Offset
				}

			}

		})

	return
}

func eachInt(ints []int, eachFunc func(index, x int) error) (err error) {
	var wg sync.WaitGroup
	for index, x := range ints {
		wg.Add(1)
		go func(index, x int) {
			defer wg.Done()
			if eachErr := eachFunc(index, x); eachErr != nil {
				err = eachErr
			}
		}(index, x)
	}
	wg.Wait()
	return
}
