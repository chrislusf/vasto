package gateway

import (
	"fmt"
	"log"
	"net"
)

type GatewayOption struct {
	Host       *string
	TcpPort    *int32
	GrpcPort   *int32
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

	if *option.GrpcPort != 0 {
		grpcListener, err := net.Listen("tcp", fmt.Sprintf("%v:%d", *option.Host, *option.GrpcPort))
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("vasto gateway starts grpc %v:%d\n", *option.Host, *option.GrpcPort)
		go gs.serveGrpc(grpcListener)
	}

	go gs.registerGatewayAtMasterServer()

	select {}

}
