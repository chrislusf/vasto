package store

import (
	"fmt"
	"log"
	"net"
	"os"

	"github.com/chrislusf/vasto/storage/rocks"
	"github.com/chrislusf/vasto/util/on_interrupt"
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
	db     *rocks.Rocks
}

func RunStore(option *StoreOption) {

	var ss = &storeServer{
		option: option,
		db:     rocks.New(option.storeFolder()),
	}

	log.Printf("Vasto store starts on %s", option.storeFolder())

	if *option.TcpPort != 0 {
		tcpListener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", *option.ListenHost, *option.TcpPort))
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("tcp     %v:%d", *option.ListenHost, *option.TcpPort)
		go ss.serveTcp(tcpListener)
	}

	if *option.GrpcPort != 0 {
		grpcListener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", *option.ListenHost, *option.GrpcPort))
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("grpc    %v:%d", *option.ListenHost, *option.GrpcPort)
		go ss.serveGrpc(grpcListener)
	}

	if *option.UnixSocket != "" {
		unixSocketListener, err := net.Listen("unix", *option.UnixSocket)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("socket   %s", *option.UnixSocket)
		on_interrupt.OnInterrupt(func() {
			os.Remove(*option.UnixSocket)
		}, nil)
		go ss.serveTcp(unixSocketListener)
	}

	go ss.keepConnectedToMasterServer()

	select {}

}

func (option *StoreOption) storeFolder() string {
	return fmt.Sprintf("%s/%d", *option.Dir, *option.Id)
}
