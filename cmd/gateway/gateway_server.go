package gateway

import (
	"fmt"
	"log"
	"net"

	"context"
	"github.com/chrislusf/vasto/client"
	"github.com/chrislusf/vasto/util/on_interrupt"
	"os"
)

type GatewayOption struct {
	TcpAddress *string
	UnixSocket *string
	Master     *string
	DataCenter *string
	Keyspace   *string
}

type gatewayServer struct {
	option *GatewayOption

	vastoClient *client.VastoClient
}

func RunGateway(option *GatewayOption) {

	var gs = &gatewayServer{
		option:      option,
		vastoClient: client.NewClient(context.Background(), "gateway", *option.Master, *option.DataCenter),
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

	gs.vastoClient.RegisterForKeyspace(*option.Keyspace)

	fmt.Printf("Vasto gateway ready\n")
	select {}

}
