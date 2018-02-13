package master

import (
	"net"
	"sync"

	"github.com/chrislusf/vasto/pb"
	"google.golang.org/grpc"
	"github.com/chrislusf/glog"
	"sync/atomic"
)

type MasterOption struct {
	Address *string
}

type masterServer struct {
	option               *MasterOption
	clientChans          *clientChannels
	clientsStat          *clientsStat
	topo                 *masterTopology
	keyspaceMutexMap     map[string]*mutexWithCounter
	keyspaceMutexMapLock sync.Mutex
}

func RunMaster(option *MasterOption) {
	var ms = &masterServer{
		option:           option,
		clientChans:      newClientChannels(),
		clientsStat:      newClientsStat(),
		topo:             newMasterTopology(),
		keyspaceMutexMap: make(map[string]*mutexWithCounter),
	}

	listener, err := net.Listen("tcp", *option.Address)
	if err != nil {
		glog.Fatal(err)
	}
	glog.V(0).Infof("Vasto master starts on %s\n", *option.Address)

	// m := cmux.New(listener)
	// grpcListener := m.Match(cmux.HTTP2HeaderField("content-type", "application/grpc"))

	ms.serveGrpc(listener)

	//if err := m.Serve(); !strings.Contains(err.Error(), "use of closed network connection") {
	//	panic(err)
	//}

}

func (ms *masterServer) serveGrpc(listener net.Listener) {
	grpcServer := grpc.NewServer()
	pb.RegisterVastoMasterServer(grpcServer, ms)
	grpcServer.Serve(listener)
}

type mutexWithCounter struct {
	sync.Mutex
	counter int64
}

func (ms *masterServer) lock(keyspace string) {
	ms.keyspaceMutexMapLock.Lock()
	mu, found := ms.keyspaceMutexMap[keyspace]
	if !found {
		mu = &mutexWithCounter{}
		ms.keyspaceMutexMap[keyspace] = mu
	}
	atomic.AddInt64(&mu.counter, 1)
	ms.keyspaceMutexMapLock.Unlock()

	mu.Lock()
}

func (ms *masterServer) unlock(keyspace string) {
	ms.keyspaceMutexMapLock.Lock()
	mu, found := ms.keyspaceMutexMap[keyspace]
	if found {
		if atomic.AddInt64(&mu.counter, -1) == 0 {
			delete(ms.keyspaceMutexMap, keyspace)
		}
	}
	ms.keyspaceMutexMapLock.Unlock()

	mu.Unlock()

}
