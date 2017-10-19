package gateway

import (
	"fmt"
	"log"
	"net"

	"github.com/chrislusf/vasto/cmd/client"
)

type GatewayOption struct {
	Host     *string
	TcpPort  *int32
	GrpcPort *int32
	// either cluster
	FixedCluster *string
	// or Master with DataCenter
	Master     *string
	DataCenter *string
}

type gatewayServer struct {
	option *GatewayOption

	vastoClient *client.VastoClient
}

func RunGateway(option *GatewayOption) {

	var gs = &gatewayServer{
		option: option,
		vastoClient: client.New(
			&client.ClientOption{
				FixedCluster: option.FixedCluster,
				Master:       option.Master,
				DataCenter:   option.DataCenter,
			},
		),
	}

	if *option.GrpcPort != 0 {
		grpcListener, err := net.Listen("tcp", fmt.Sprintf("%v:%d", *option.Host, *option.GrpcPort))
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Vasto gateway starts grpc %v:%d\n", *option.Host, *option.GrpcPort)
		go gs.serveGrpc(grpcListener)
	}

	clientReadyChan := make(chan bool)
	go gs.vastoClient.Start(clientReadyChan)
	<-clientReadyChan

	fmt.Printf("Vasto gateway ready\n")
	select {}

}
