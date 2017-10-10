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
	Address *string
}

type masterServer struct {
	option      *MasterOption
	clientChans *clientChannels
	rings       map[string]topology.Ring
	sync.Mutex
}

func RunMaster(option *MasterOption) {
	var ms = &masterServer{
		option:      option,
		clientChans: newClientChannels(),
		rings:       make(map[string]topology.Ring),
	}

	listener, err := net.Listen("tcp", *option.Address)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Vasto master starts on %s\n", *option.Address)

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
