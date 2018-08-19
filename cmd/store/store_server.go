package store

import (
	"fmt"
	"net"
	"os"

	"context"
	"github.com/chrislusf/glog"
	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/topology/clusterlistener"
	"github.com/chrislusf/vasto/util"
	"github.com/chrislusf/vasto/util/interrupt"
	"sync"
)

// StoreOption has options to run a data store
type StoreOption struct {
	Dir               *string
	Host              *string
	ListenHost        *string
	TcpPort           *int32
	Bootstrap         *bool
	DisableUnixSocket *bool
	Master            *string
	DataCenter        *string
	LogFileSizeMb     *int
	LogFileCount      *int
	DiskSizeGb        *int
	Tags              *string
	DisableUseEventIo *bool
	DisableBinLog     *bool
}

// GetAdminPort returns the admin port of the store, which is the data port plus 10000
func (o *StoreOption) GetAdminPort() int32 {
	return *o.TcpPort + 10000
}

type storeServer struct {
	option              *StoreOption
	clusterListener     *clusterlistener.ClusterListener
	ShardInfoChan       chan *pb.ShardInfo
	statusInCluster     map[string]*pb.LocalShardsInCluster // saved to disk
	statusInClusterLock sync.RWMutex
	periodTasks         []periodicTask
	keyspaceShards      *keyspaceShards
	storeName           string
}

// RunStore starts a store process
func RunStore(option *StoreOption) {

	storeName := fmt.Sprintf("[store@%s:%d]", *option.ListenHost, *option.TcpPort)

	ctx := context.Background()
	clusterListener := clusterlistener.NewClusterListener(*option.DataCenter, storeName)

	var ss = &storeServer{
		option:          option,
		clusterListener: clusterListener,
		ShardInfoChan:   make(chan *pb.ShardInfo),
		statusInCluster: make(map[string]*pb.LocalShardsInCluster),
		keyspaceShards:  newKeyspaceShards(),
		storeName:       storeName,
	}
	go ss.startPeriodTasks()

	// ss.clusterListener.RegisterShardEventProcessor(&clusterlistener.ClusterEventLogger{})

	if err := ss.listExistingClusters(); err != nil {
		glog.Fatalf("%s load existing cluster files: %v", ss.storeName, err)
	}

	// connect to the master
	go ss.keepConnectedToMasterServer(ctx)
	for keyspaceName, ShardInfo := range ss.statusInCluster {
		clusterListener.AddExistingKeyspace(keyspaceName, int(ShardInfo.ClusterSize), int(ShardInfo.ReplicationFactor))
	}
	clusterListener.StartListener(ctx, *ss.option.Master, *ss.option.DataCenter)

	for keyspaceName, storeStatus := range ss.statusInCluster {
		if err := ss.startExistingNodes(keyspaceName, storeStatus); err != nil {
			glog.Fatalf("%s load existing keyspace %v: %v", ss.storeName, keyspaceName, err)
		}
	}

	if *option.TcpPort != 0 {
		grpcAddress := fmt.Sprintf("%s:%d", *option.ListenHost, option.GetAdminPort())
		grpcListener, err := net.Listen("tcp", grpcAddress)
		if err != nil {
			glog.Fatal(err)
		}
		glog.V(0).Infof("%s store admin %s", ss.storeName, grpcAddress)
		go ss.serveGrpc(grpcListener)
	}

	if *option.TcpPort != 0 {
		tcpAddress := fmt.Sprintf("%s:%d", *option.ListenHost, *option.TcpPort)
		tcpListener, err := net.Listen("tcp", tcpAddress)
		if err != nil {
			glog.Fatal(err)
		}
		glog.V(2).Infof("%s listens on tcp %v", ss.storeName, tcpAddress)
		go ss.serveTcp(tcpListener)
	}

	if !*option.DisableUnixSocket {
		tcpAddress := fmt.Sprintf("%s:%d", *option.ListenHost, *option.TcpPort)
		if unixSocket, _ := util.GetUnixSocketFile(tcpAddress); unixSocket != "" {
			if util.FileExists(unixSocket) {
				os.Remove(unixSocket)
			}
			unixSocketListener, err := net.Listen("unix", unixSocket)
			if err != nil {
				glog.Fatal(err)
			}
			glog.V(2).Infof("listens on socket %s", unixSocket)
			interrupt.OnInterrupt(func() {
				os.Remove(unixSocket)
			}, nil)
			defer os.Remove(unixSocket)
			go ss.serveTcp(unixSocketListener)
		}
	}

	glog.V(2).Infof("%s Vasto store starts on %s", ss.storeName, *option.Dir)

	select {}

}
