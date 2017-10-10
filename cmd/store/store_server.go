package store

import (
	"fmt"
	"github.com/chrislusf/vasto/util/on_interrupt"
	"log"
	"net"
	"os"
)

type StoreOption struct {
	Id         *int32
	Dir        *string
	Host       *string
	ListenHost *string
	TcpPort    *int32
	UnixSocket *string
	GrpcPort   *int32
	Master     *string
	DataCenter *string
}

type storeServer struct {
	option *StoreOption
}

func RunStore(option *StoreOption) {

	var ss = &storeServer{
		option: option,
	}

	startsStatus := "Vasto store starts on"

	if *option.TcpPort != 0 {
		tcpListener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", *option.ListenHost, *option.TcpPort))
		if err != nil {
			log.Fatal(err)
		}
		startsStatus += fmt.Sprintf("\n tcp     %v:%d", *option.ListenHost, *option.TcpPort)
		go ss.serveTcp(tcpListener)
	}

	if *option.GrpcPort != 0 {
		grpcListener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", *option.ListenHost, *option.GrpcPort))
		if err != nil {
			log.Fatal(err)
		}
		startsStatus += fmt.Sprintf("\n grpc    %v:%d", *option.ListenHost, *option.GrpcPort)
		go ss.serveGrpc(grpcListener)
	}

	if *option.UnixSocket != "" {
		unixSocketListener, err := net.Listen("unix", *option.UnixSocket)
		if err != nil {
			log.Fatal(err)
		}
		startsStatus += fmt.Sprintf("\n socket   %s", *option.UnixSocket)
		on_interrupt.OnInterrupt(func() {
			os.Remove(*option.UnixSocket)
		}, nil)
		go ss.serveTcp(unixSocketListener)
	}

	log.Println(startsStatus)

	go ss.keepConnectedToMasterServer()

	select {}

}
