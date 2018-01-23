package master

import (
	"context"
	"fmt"
	"github.com/chrislusf/vasto/pb"
	"google.golang.org/grpc"
	"log"
	"math"
	"sort"
	"sync"
)

// allocateServers
// 1. select servers that has all the requiredTags and enough disk
// 2. sort by free capacity desc
// 3. pick the top n
// the actual capacity is deducted until the stores create the database and report to the master
func (dc *dataCenter) allocateServers(n int, totalGb float64, filterFunc func(*pb.StoreResource) bool) (stores []*pb.StoreResource, err error) {
	var servers []*pb.StoreResource

	eachRequiredGb := uint32(math.Ceil(totalGb / float64(n)))

	// 1. select servers that has all the requiredTags and enough disk
	dc.RLock()
	for _, server := range dc.servers {
		if filterFunc(server) && (server.DiskSizeGb-server.AllocatedSizeGb) > eachRequiredGb {
			servers = append(servers, server)
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

func createShards(ctx context.Context, keyspace string, clusterSize, replicationFactor, eachShardSizeGb uint32, stores []*pb.StoreResource) error {

	return eachStore(stores, func(serverId int, store *pb.StoreResource) error {
		// log.Printf("connecting to server %d at %s", serverId, store.GetAdminAddress())
		return withConnection(store, func(grpcConnection *grpc.ClientConn) error {

			client := pb.NewVastoStoreClient(grpcConnection)
			request := &pb.CreateShardRequest{
				Keyspace:          keyspace,
				ServerId:          uint32(serverId),
				ClusterSize:       clusterSize,
				ReplicationFactor: replicationFactor,
				ShardDiskSizeGb:   eachShardSizeGb,
			}

			log.Printf("create shard on %v: %v", store.AdminAddress, request)
			resp, err := client.CreateShard(ctx, request)
			if err != nil {
				return err
			}
			if resp.Error != "" {
				return fmt.Errorf("create shard %d on %s: %s", serverId, store.AdminAddress, resp.Error)
			}
			return nil
		})
	})
}

func deleteShards(ctx context.Context, req *pb.DeleteClusterRequest, stores []*pb.StoreResource) error {

	return eachStore(stores, func(serverId int, store *pb.StoreResource) error {
		// log.Printf("connecting to server %d at %s", serverId, store.GetAdminAddress())
		return withConnection(store, func(grpcConnection *grpc.ClientConn) error {

			client := pb.NewVastoStoreClient(grpcConnection)
			request := &pb.DeleteKeyspaceRequest{
				Keyspace: req.Keyspace,
			}

			log.Printf("delete keyspace on %v: %v", store.AdminAddress, request)
			resp, err := client.DeleteKeyspace(ctx, request)
			if err != nil {
				return err
			}
			if resp.Error != "" {
				return fmt.Errorf("delete keyspace %s on %s: %s", req.Keyspace, store.AdminAddress, resp.Error)
			}
			return nil
		})
	})

}

func withConnection(store *pb.StoreResource, fn func(*grpc.ClientConn) error) error {

	grpcConnection, err := grpc.Dial(store.GetAdminAddress(), grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("fail to dial %s: %v", store.GetAdminAddress(), err)
	}
	defer grpcConnection.Close()

	return fn(grpcConnection)
}

func eachStore(stores []*pb.StoreResource, eachFunc func(serverId int, store *pb.StoreResource) error) (err error) {
	var wg sync.WaitGroup
	for serverId, store := range stores {
		wg.Add(1)
		go func(serverId int, store *pb.StoreResource) {
			defer wg.Done()
			if eachErr := eachFunc(serverId, store); eachErr != nil {
				err = eachErr
			}
		}(serverId, store)
	}
	wg.Wait()
	return
}
