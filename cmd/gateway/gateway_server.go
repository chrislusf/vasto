package gateway

import (
	"fmt"
	"log"
	"net"

	"github.com/chrislusf/vasto/cmd/client"
	"github.com/chrislusf/vasto/util/on_interrupt"
	"os"
)

type GatewayOption struct {
	TcpAddress *string
	UnixSocket *string
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

	if *option.TcpAddress != "" {
		tcpListener, err := net.Listen("tcp", *option.TcpAddress)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Vasto gateway listens on tcp %s\n", *option.TcpAddress)
		go gs.serveTcp(tcpListener)
	}

	if *option.UnixSocket != "" {
		unixSocketListener, err := net.Listen("unix", *option.UnixSocket)
		if err != nil {
			log.Printf("Vasto gateway starts on socket %s", *option.UnixSocket)
			log.Fatal(err)
		}
		log.Printf("Vasto gateway listens on socket %s", *option.UnixSocket)
		on_interrupt.OnInterrupt(func() {
			os.Remove(*option.UnixSocket)
		}, nil)
		go gs.serveTcp(unixSocketListener)
	}

	gs.vastoClient.Start()

	fmt.Printf("Vasto gateway ready\n")
	select {}

}
