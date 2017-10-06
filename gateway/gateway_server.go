package gateway

import (
	"fmt"
	"log"
	"net"
	"strings"

	"github.com/soheilhy/cmux"
)

type GatewayOption struct {
	Host       *string
	Port       *int32
	Master     *string
	DataCenter *string
}

type gatewayServer struct {
	option *GatewayOption
}

func RunGateway(option *GatewayOption) {

	var gs = &gatewayServer{
		option: option,
	}

	listener, err := net.Listen("tcp", fmt.Sprintf("%v:%d", *option.Host, *option.Port))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("vasto gateway starts on %v:%d\n", *option.Host, *option.Port)

	m := cmux.New(listener)
	grpcListener := m.Match(cmux.HTTP2HeaderField("content-type", "application/grpc"))

	go gs.serveGrpc(grpcListener)

	go gs.registerGatewayAtMasterServer()

	if err := m.Serve(); !strings.Contains(err.Error(), "use of closed network connection") {
		panic(err)
	}

}
