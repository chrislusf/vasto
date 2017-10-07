package master

import (
	"fmt"
	"log"
	"net"
	"strings"

	"github.com/chrislusf/vasto/pb"
	"github.com/soheilhy/cmux"
)

type MasterOption struct {
	Address *string
}

type masterServer struct {
	option *MasterOption
}

func RunMaster(option *MasterOption) {
	var ms = &masterServer{
		option: option,
	}

	listener, err := net.Listen("tcp", *option.Address)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("vasto master starts on %s\n", *option.Address)

	m := cmux.New(listener)
	grpcListener := m.Match(cmux.HTTP2HeaderField("content-type", "application/grpc"))

	go ms.serveGrpc(grpcListener)

	if err := m.Serve(); !strings.Contains(err.Error(), "use of closed network connection") {
		panic(err)
	}

}

func (m *masterServer) addStore(dataCenter, groupName string, store *pb.StoreResource) {

}

func (m *masterServer) start() {

}
