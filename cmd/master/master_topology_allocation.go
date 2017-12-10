package master

import (
	"github.com/chrislusf/vasto/pb"
	"sort"
	"math"
	"fmt"
	"context"
	"sync"
	"google.golang.org/grpc"
)

// allocateServers
// 1. select servers that has all the requiredTags and enough disk
// 2. sort by free capacity desc
// 3. pick the top n
// the actual capacity is deducted until the stores create the database and report to the master
func (dc *dataCenter) allocateServers(n int, totalGb float64, requiredTags []string) (stores []*pb.StoreResource, err error) {
	var servers []*pb.StoreResource

	eachRequiredGb := uint32(math.Ceil(totalGb / float64(n)))

	// 1. select servers that has all the requiredTags and enough disk
	dc.RLock()
	for _, server := range dc.servers {
		t := server
		if meetRequirement(t.Tags, requiredTags) && (t.DiskSizeGb-t.AllocatedSizeGb) > eachRequiredGb {
			servers = append(servers, t)
		}
	}
	dc.RUnlock()

	if len(servers) < n {
		return nil, fmt.Errorf("only has %d servers meet the requirement", len(servers))
	}

	// 2. sort by free capacity desc
	sort.Slice(servers, func(i, j int) bool {
		return (servers[i].DiskSizeGb - servers[i].AllocatedSizeGb) >= (servers[j].DiskSizeGb - servers[j].AllocatedSizeGb)
	})

	// 3. pick the top n
	return servers[:n], nil
}

func meetRequirement(existingTags, requiredTags []string) bool {
	for _, tag := range requiredTags {
		found := false
		for _, et := range existingTags {
			if et == tag {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func createShards(ctx context.Context, req *pb.CreateClusterRequest, stores []*pb.StoreResource) (error) {

	var createShardsError error
	var wg sync.WaitGroup
	for shardId, store := range stores {
		wg.Add(1)
		go func(shardId int, store *pb.StoreResource) {
			defer wg.Done()
			// log.Printf("connecting to server %d at %s", shardId, store.GetAdminAddress())
			if err := withConnection(store, func(grpcConnection *grpc.ClientConn) error {

				client := pb.NewVastoStoreClient(grpcConnection)
				request := &pb.CreateShardRequest{
					Keyspace:          req.Keyspace,
					ShardId:           uint32(shardId),
					ClusterSize:       req.ClusterSize,
					ReplicationFactor: req.ReplicationFactor,
					ShardDiskSizeGb:   uint32(math.Ceil(float64(req.TotalDiskSizeGb) / float64(req.ClusterSize))),
				}

				// log.Printf("sending store %v: %v", store.AdminAddress, request)
				resp, err := client.CreateShard(ctx, request)
				if err != nil {
					return err
				}
				if resp.Error != "" {
					return fmt.Errorf("create shard %d on %s: %s", shardId, store.AdminAddress, resp.Error)
				}
				return nil
			}); err != nil {
				createShardsError = err
				return
			}
			// log.Printf("shard created on server %d at %s", shardId, store.GetAdminAddress())
		}(shardId, store)
	}
	wg.Wait()
	return createShardsError
}

func deleteShards(ctx context.Context, req *pb.DeleteClusterRequest, stores []*pb.StoreResource) (error) {

	var deleteShardsError error
	var wg sync.WaitGroup
	for shardId, store := range stores {
		wg.Add(1)
		go func(shardId int, store *pb.StoreResource) {
			defer wg.Done()
			// log.Printf("connecting to server %d at %s", shardId, store.GetAdminAddress())
			if err := withConnection(store, func(grpcConnection *grpc.ClientConn) error {

				client := pb.NewVastoStoreClient(grpcConnection)
				request := &pb.DeleteKeyspaceRequest{
					Keyspace: req.Keyspace,
				}

				// log.Printf("sending store %v: %v", store.AdminAddress, request)
				resp, err := client.DeleteKeyspace(ctx, request)
				if err != nil {
					return err
				}
				if resp.Error != "" {
					return fmt.Errorf("delete keyspace %s on %s: %s", req.Keyspace, store.AdminAddress, resp.Error)
				}
				return nil
			}); err != nil {
				deleteShardsError = err
				return
			}
			// log.Printf("shard created on server %d at %s", shardId, store.GetAdminAddress())
		}(shardId, store)
	}
	wg.Wait()
	return deleteShardsError
}

func withConnection(store *pb.StoreResource, fn func(*grpc.ClientConn) error) error {

	grpcConnection, err := grpc.Dial(store.GetAdminAddress(), grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("fail to dial %s: %v", store.GetAdminAddress(), err)
	}
	defer grpcConnection.Close()

	return fn(grpcConnection)
}
