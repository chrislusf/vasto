package gateway

import (
	"fmt"
	"net"

	"context"
	"github.com/chrislusf/glog"
	"github.com/chrislusf/vasto/util/interrupt"
	"github.com/chrislusf/vasto/goclient/vs"
	"os"
)

// GatewayOption has options to run gateway
type GatewayOption struct {
	TcpAddress *string
	UnixSocket *string
	Master     *string
	DataCenter *string
	Keyspace   *string
}

type gatewayServer struct {
	option *GatewayOption

	vastoClient *vs.VastoClient
}

// RunGateway starts a gateway process
func RunGateway(option *GatewayOption) {

	var gs = &gatewayServer{
		option:      option,
		vastoClient: vs.NewVastoClient(context.Background(), "gateway", *option.Master, *option.DataCenter),
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
		interrupt.OnInterrupt(func() {
			os.Remove(*option.UnixSocket)
		}, nil)
		go gs.serveTcp(unixSocketListener)
	}

	gs.vastoClient.NewClusterClient(*option.Keyspace)

	glog.V(0).Infof("Vasto gateway ready\n")
	select {}

}
