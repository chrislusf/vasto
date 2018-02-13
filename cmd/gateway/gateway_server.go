package gateway

import (
	"fmt"
	"net"

	"context"
	"github.com/chrislusf/vasto/client"
	"github.com/chrislusf/vasto/util/on_interrupt"
	"os"
	"github.com/chrislusf/glog"
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
			glog.Fatal(err)
		}
		fmt.Printf("Vasto gateway listens on tcp %s\n", *option.TcpAddress)
		go gs.serveTcp(tcpListener)
	}

	if *option.UnixSocket != "" {
		unixSocketListener, err := net.Listen("unix", *option.UnixSocket)
		if err != nil {
			glog.Errorf("Vasto gateway starts on socket %s", *option.UnixSocket)
			glog.Fatal(err)
		}
		glog.V(0).Infof("Vasto gateway listens on socket %s", *option.UnixSocket)
		on_interrupt.OnInterrupt(func() {
			os.Remove(*option.UnixSocket)
		}, nil)
		go gs.serveTcp(unixSocketListener)
	}

	gs.vastoClient.GetClusterClient(*option.Keyspace)

	glog.V(0).Infof("Vasto gateway ready\n")
	select {}

}
