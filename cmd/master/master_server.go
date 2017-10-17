package master

import (
	"fmt"
	"log"
	"net"
	//"strings"

	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/topology"
	//"github.com/soheilhy/cmux"
	"google.golang.org/grpc"
	"sync"
)

type MasterOption struct {
	Address     *string
	ClusterSize *int32
}

type masterServer struct {
	option      *MasterOption
	clientChans *clientChannels
	rings       map[string]topology.Cluster
	sync.Mutex
	currentClusterSize uint32
	nextClusterSize    uint32
}

func RunMaster(option *MasterOption) {
	var ms = &masterServer{
		option:             option,
		clientChans:        newClientChannels(),
		rings:              make(map[string]topology.Cluster),
		currentClusterSize: uint32(*option.ClusterSize),
	}

	listener, err := net.Listen("tcp", *option.Address)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Vasto master starts on %s, cluster size %d\n", *option.Address, *option.ClusterSize)

	// m := cmux.New(listener)
	// grpcListener := m.Match(cmux.HTTP2HeaderField("content-type", "application/grpc"))

	ms.serveGrpc(listener)

	//if err := m.Serve(); !strings.Contains(err.Error(), "use of closed network connection") {
	//	panic(err)
	//}

}

func (ms *masterServer) serveGrpc(listener net.Listener) {
	grpcServer := grpc.NewServer()
	pb.RegisterVastoMasterServer(grpcServer, ms)
	grpcServer.Serve(listener)
}
