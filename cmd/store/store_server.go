package store

import (
	"fmt"
	"log"
	"net"
)

type StoreOption struct {
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

	if *option.TcpPort != 0 {
		tcpListener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", *option.ListenHost, *option.TcpPort))
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("vasto store starts tcp %v:%d\n", *option.ListenHost, *option.TcpPort)
		go ss.serveTcp(tcpListener)
	}

	if *option.GrpcPort != 0 {
		grpcListener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", *option.ListenHost, *option.GrpcPort))
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("vasto store starts grpc %v:%d\n", *option.ListenHost, *option.GrpcPort)
		go ss.serveGrpc(grpcListener)
	}

	if *option.UnixSocket != "" {
		unixSocketListener, err := net.Listen("unix", *option.UnixSocket)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("vasto store starts socket %s\n", *option.UnixSocket)
		go ss.serveTcp(unixSocketListener)
	}

	go ss.registerAtMasterServer()

	select {}

}
