package store

import (
	"fmt"
	"log"
	"net"
	"strings"

	// "github.com/chrislusf/vasto/pb"
	"github.com/soheilhy/cmux"
)

type StoreOption struct {
	Dir        *string
	Host       *string
	Port       *int32
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

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", *option.Port))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("vasto store starts on %v:%d\n", *option.Host, *option.Port)

	m := cmux.New(listener)
	// grpcListener := m.Match(cmux.HTTP2HeaderField("content-type", "application/grpc"))
	tcpListener := m.Match(cmux.Any())

	// go ss.serveGrpc(grpcListener)
	go ss.serveTcp(tcpListener)

	go ss.registerAtMasterServer()

	if err := m.Serve(); !strings.Contains(err.Error(), "use of closed network connection") {
		panic(err)
	}

}
