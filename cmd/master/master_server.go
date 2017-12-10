package master

import (
	"log"
	"net"
	//"strings"

	"github.com/chrislusf/vasto/pb"
	//"github.com/soheilhy/cmux"
	"google.golang.org/grpc"
)

type MasterOption struct {
	Address *string
}

type masterServer struct {
	option      *MasterOption
	clientChans *clientChannels
	clientsStat *clientsStat
	topo        *masterTopology
}

func RunMaster(option *MasterOption) {
	var ms = &masterServer{
		option:      option,
		clientChans: newClientChannels(),
		clientsStat: newClientsStat(),
		topo:        newMasterTopology(),
	}

	listener, err := net.Listen("tcp", *option.Address)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Vasto master starts on %s\n", *option.Address)

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
