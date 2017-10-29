package store

import (
	"fmt"
	"log"
	"net"
	"os"

	"github.com/chrislusf/vasto/storage/change_log"
	"github.com/chrislusf/vasto/storage/rocks"
	"github.com/chrislusf/vasto/util/on_interrupt"
)

type StoreOption struct {
	Id            *int32
	Dir           *string
	Host          *string
	ListenHost    *string
	TcpPort       *int32
	UnixSocket    *string
	AdminPort     *int32
	Master        *string
	DataCenter    *string
	LogFileSizeMb *int
	LogFileCount  *int
}

type storeServer struct {
	option *StoreOption
	db     *rocks.Rocks
	lm     *change_log.LogManager
}

func RunStore(option *StoreOption) {

	var ss = &storeServer{
		option: option,
		db:     rocks.New(option.storeFolder()),
	}
	if *option.LogFileSizeMb > 0 {
		ss.lm = change_log.NewLogManager(*option.Dir, int64(*option.LogFileSizeMb*1024*1024), *option.LogFileCount)
		ss.lm.Initialze()
	}

	log.Printf("Vasto store starts on %s", option.storeFolder())
	if *option.AdminPort != 0 {
		grpcListener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", *option.ListenHost, *option.AdminPort))
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("store admin %s:%d", *option.ListenHost, *option.AdminPort)
		go ss.serveGrpc(grpcListener)
	}

	if *option.TcpPort != 0 {
		tcpAddress := fmt.Sprintf("%s:%d", *option.ListenHost, *option.TcpPort)
		tcpListener, err := net.Listen("tcp", tcpAddress)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("listens on tcp %v", tcpAddress)
		go ss.serveTcp(tcpListener)
	}

	if *option.UnixSocket != "" {
		unixSocketListener, err := net.Listen("unix", *option.UnixSocket)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("listens on socket %s", *option.UnixSocket)
		on_interrupt.OnInterrupt(func() {
			os.Remove(*option.UnixSocket)
		}, nil)
		go ss.serveTcp(unixSocketListener)
	}

	if *option.Master != "" {
		go ss.keepConnectedToMasterServer()
	}

	select {}

}

func (option *StoreOption) storeFolder() string {
	return fmt.Sprintf("%s/%d", *option.Dir, *option.Id)
}
