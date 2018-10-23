package store

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/chrislusf/glog"
	"github.com/chrislusf/gorocksdb"
	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/storage/codec"
	"github.com/chrislusf/vasto/topology"
	"github.com/chrislusf/vasto/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
		// glog.V(2).Infof("bootstrap shard %d is not needed", s.id)
		return nil
	}

	glog.V(1).Infof("%s bootstrap from server %+v ...", s, bestPeerToCopy)

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

	if bootstrapPlan.PickBestBootstrapSource {

		bestPeerToCopy, isNeeded := s.isBootstrapNeeded(ctx, bootstrapPlan)
		if !isNeeded {
			// glog.V(2).Infof("bootstrap shard %d is not needed", s.id)
			return nil
		}

		glog.V(1).Infof("bootstrap %s from %s ...", s, bestPeerToCopy)

		return topology.VastoNodes(existingPrimaryShards).WithConnection(fmt.Sprintf("%s bootstrap from one exisiting %d.%d", s.String(), bestPeerToCopy.ServerId, bestPeerToCopy.ShardId),
			bestPeerToCopy.ServerId, func(node *pb.ClusterNode, grpcConnection *grpc.ClientConn) error {
				return s.doBootstrapCopy(ctx, grpcConnection, node, bootstrapPlan.FromClusterSize, bootstrapPlan.ToClusterSize, int(s.id))
			})
	}

	var bootstrapSourceServerIds []int
	var sourceRowChans []chan *pb.RawKeyValue
	for _, shard := range bootstrapPlan.BootstrapSource {
		bootstrapSourceServerIds = append(bootstrapSourceServerIds, shard.ServerId)
		sourceRowChans = append(sourceRowChans, make(chan *pb.RawKeyValue, constBootstrapCopyBatchSize))
	}

	return util.Parallel(
		func() error {
			return eachInt(bootstrapSourceServerIds, func(index, serverId int) error {
				sourceChan := sourceRowChans[index]
				defer close(sourceChan)
				return topology.VastoNodes(existingPrimaryShards).WithConnection(
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
				glog.V(1).Infof("bootstrap %v via sst ...", s.String())
				return s.db.AddSstByWriter(fmt.Sprintf("%s bootstrapCopy write", s.String()),
					func(w *gorocksdb.SSTFileWriter) (int64, error) {
						counter, err := pb.MergeSorted(sourceRowChans, 0, func(keyValue *pb.RawKeyValue) error {

							if err := w.Add(keyValue.Key, keyValue.Value); err != nil {
								return fmt.Errorf("add to sst: %v", err)
							}
							return nil
						})
						return counter, err
					},
				)
			}

			// slow way to backfill
			glog.V(1).Infof("bootstrap %v via sorted insert ...", s.String())
			glog.V(1).Infof("life file size before: %d", s.db.LiveFilesSize())

			var receivedCounter int64
			var newCounter int
			var expiredCounter int
			var updatedCounter int
			var skippedCounter int

			receivedCounter, err := pb.MergeSorted(sourceRowChans, 0, func(keyValue *pb.RawKeyValue) error {

				b, err := s.db.Get(keyValue.Key)
				if err != nil || len(b) == 0 {
					newCounter++
					return s.db.Put(keyValue.Key, keyValue.Value)
				}

				existingRow := codec.FromBytes(b)
				if existingRow.IsExpired() {
					expiredCounter++
					return s.db.Put(keyValue.Key, keyValue.Value)
				}

				incomingRow := codec.FromBytes(keyValue.Value)
				if existingRow.UpdatedAtNs < incomingRow.UpdatedAtNs {
					updatedCounter++
					return s.db.Put(keyValue.Key, keyValue.Value)
				}

				skippedCounter++

				return nil

			})

			glog.V(1).Infof("bootstrap %v via sorted insert processed %d entries", s.String(), receivedCounter)
			glog.V(1).Infof("new entries: %d", newCounter)
			glog.V(1).Infof("expired entries: %d", expiredCounter)
			glog.V(1).Infof("updated entries: %d", updatedCounter)
			glog.V(1).Infof("skipped entries: %d", skippedCounter)

			glog.V(1).Infof("life file size after: %d", s.db.LiveFilesSize())

			return err

		},
	)

}

func (s *shard) checkBinlogAvailable(ctx context.Context, grpcConnection *grpc.ClientConn, node *pb.ClusterNode) (latestSegment uint32, canTailBinlog bool, err error) {

	segment, _, hasProgress, err := s.loadProgress(node.StoreResource.GetAdminAddress(), s.id)

	glog.V(1).Infof("checkBinlogAvailable: shard %d segment %d hasProgress:%v, err: %v", s.id, segment, hasProgress, err)

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

	glog.V(1).Infof("bootstrap %s from %s %s filter by %d/%d", s.String(), node.StoreResource.Address, node.ShardInfo.IdentifierOnThisServer(), targetShardId, targetClusterSize)

	s.db.Close()
	s.db.Destroy()
	s.db.EnsureDirectory()
	s.db.Reopen()

	counter, segment, offset, err := s.writeToSst(ctx, grpcConnection, node.ShardInfo, clusterSize, targetClusterSize, targetShardId)

	glog.V(1).Infof("bootstrap %s from %s %s filter by %d/%d received %d entries", s.String(), node.StoreResource.Address, node.ShardInfo.IdentifierOnThisServer(), targetShardId, targetClusterSize, counter)

	if err != nil {
		return fmt.Errorf("writeToSst: %v", err)
	}

	return s.saveProgress(node.StoreResource.GetAdminAddress(), VastoShardId(node.ShardInfo.ShardId), segment, offset)

}

func (s *shard) doBootstrapCopy2(ctx context.Context, grpcConnection *grpc.ClientConn, node *pb.ClusterNode, clusterSize, targetClusterSize int, targetShardId int, rowChan chan *pb.RawKeyValue) (err error) {

	glog.V(1).Infof("bootstrap2 %s from %s %s filter by %d/%d", s.String(), node.StoreResource.Address, node.ShardInfo.IdentifierOnThisServer(), targetShardId, targetClusterSize)

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
	var counter int

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
			counter++

		}

		if response.BinlogTailProgress != nil {
			segment = response.BinlogTailProgress.Segment
			offset = response.BinlogTailProgress.Offset
		}

	}

	glog.V(1).Infof("bootstrap2 %s from %s received %d entries, segment:offset=%d:%d : %v", s.String(), node.ShardInfo.IdentifierOnThisServer(), counter, segment, offset, err)
	s.saveProgress(node.StoreResource.GetAdminAddress(), VastoShardId(node.ShardInfo.ShardId), segment, offset)

	return
}

func (s *shard) writeToSst(ctx context.Context, grpcConnection *grpc.ClientConn, sourceShardInfo *pb.ShardInfo, clusterSize, targetClusterSize int, targetShardId int) (counter int64, segment uint32, offset uint64, err error) {

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
		st := status.Convert(err)
		for st.Code() == codes.Unavailable {
			glog.V(1).Infof("%s waits on %s ...", s, sourceShardInfo)
			time.Sleep(5 * time.Second)
			stream, err = client.BootstrapCopy(ctx, request)
			st = status.Convert(err)
		}
		return 0, 0, 0, fmt.Errorf("client.BootstrapCopy: %v", err)
	}

	err = s.db.AddSstByWriter(fmt.Sprintf("bootstrap %s from %s %d/%d", s.String(), sourceShardInfo.IdentifierOnThisServer(), targetShardId, targetClusterSize),

		func(w *gorocksdb.SSTFileWriter) (int64, error) {

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
