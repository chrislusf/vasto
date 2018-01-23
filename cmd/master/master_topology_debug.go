package master

import (
	"context"
	"fmt"
	"github.com/chrislusf/vasto/pb"
	"google.golang.org/grpc"
)

func (k *keyspace) debug(prefix string) {
	k.RLock()
	for dcName, cluster := range k.clusters {
		fmt.Printf("%s  data_center: %v %v\n", prefix, dcName, cluster.String())
		cluster.Debug(prefix + "    ")
	}
	k.RUnlock()
	return
}

func (dc *dataCenter) debug(prefix string) {
	dc.RLock()
	for serverAddress, storeResource := range dc.servers {
		fmt.Printf("%s  address: %v\n", prefix, serverAddress)
		fmt.Printf("%s    %+v\n", prefix, storeResource)
		withConnect(storeResource, func(grpcConnection *grpc.ClientConn) error {
			client := pb.NewVastoStoreClient(grpcConnection)
			client.DebugStore(context.Background(), &pb.Empty{})
			return nil
		})
	}
	dc.RUnlock()
	return
}

func (topo *masterTopology) Debug() {

	topo.keyspaces.RLock()
	for keyspaceName, keyspace := range topo.keyspaces.keyspaces {
		fmt.Printf("keyspace: %v\n", keyspaceName)
		keyspace.debug(" ")
	}
	topo.keyspaces.RUnlock()

	topo.dataCenters.RLock()
	for dcName, dc := range topo.dataCenters.dataCenters {
		fmt.Printf("data_center: %v\n", dcName)
		dc.debug(" ")
	}
	topo.dataCenters.RUnlock()
}

func withConnect(node *pb.StoreResource, fn func(*grpc.ClientConn) error) error {

	grpcConnection, err := grpc.Dial(node.AdminAddress, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("fail to dial %s: %v", node.AdminAddress, err)
	}
	defer grpcConnection.Close()

	return fn(grpcConnection)
}
