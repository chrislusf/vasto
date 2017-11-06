package store

import (
	"fmt"
	"log"
	"net"
	"os"

	"github.com/chrislusf/vasto/topology/cluster_listener"
	"github.com/chrislusf/vasto/util/on_interrupt"
)

type StoreOption struct {
	Id                *int32
	Dir               *string
	Host              *string
	ListenHost        *string
	TcpPort           *int32
	UnixSocket        *string
	AdminPort         *int32
	Master            *string
	FixedCluster      *string
	DataCenter        *string
	LogFileSizeMb     *int
	LogFileCount      *int
	ReplicationFactor *int
}

type storeServer struct {
	option          *StoreOption
	nodes           []*node
	clusterListener *cluster_listener.ClusterListener
}

func RunStore(option *StoreOption) {

	clusterListener := cluster_listener.NewClusterClient(*option.DataCenter)

	var ss = &storeServer{
		option:          option,
		clusterListener: clusterListener,
	}

	if *option.FixedCluster != "" {
		clusterListener.SetNodes(*ss.option.FixedCluster)
	} else if *option.Master != "" {
		go ss.keepConnectedToMasterServer()
		clusterListener.Start(*ss.option.Master, *ss.option.DataCenter)
	}

	nodes, err := newNodes(option, clusterListener)
	if err != nil {
		log.Fatal(err)
	}
	ss.nodes = nodes

	if *option.AdminPort != 0 {
		grpcListener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", *option.ListenHost, *option.AdminPort))
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("store admin %s:%d", *option.ListenHost, *option.AdminPort)
		go ss.serveGrpc(grpcListener)
	}

	if *option.TcpPort != 0 {
		tcpAddress := fmt.Sprintf("%s:%d", *option.ListenHost, *option.TcpPort)
		tcpListener, err := net.Listen("tcp", tcpAddress)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("listens on tcp %v", tcpAddress)
		go ss.serveTcp(tcpListener)
	}

	if *option.UnixSocket != "" {
		unixSocketListener, err := net.Listen("unix", *option.UnixSocket)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("listens on socket %s", *option.UnixSocket)
		on_interrupt.OnInterrupt(func() {
			os.Remove(*option.UnixSocket)
		}, nil)
		defer os.Remove(*option.UnixSocket)
		go ss.serveTcp(unixSocketListener)
	}

	log.Printf("Vasto store starts on %s", *option.Dir)

	select {}

}
