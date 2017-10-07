// Copyright © 2017 Chris Lu <chris.lu@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"log"
	"os"
	"runtime/pprof"

	b "github.com/chrislusf/vasto/cmd/benchmark"
	g "github.com/chrislusf/vasto/cmd/gateway"
	m "github.com/chrislusf/vasto/cmd/master"
	s "github.com/chrislusf/vasto/cmd/store"
	"github.com/chrislusf/vasto/util"
	"github.com/chrislusf/vasto/util/on_interrupt"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	app = kingpin.New("vasto", "a distributed fast key-value store")

	master       = app.Command("master", "Start a master process")
	masterOption = &m.MasterOption{
		Address: master.Flag("address", "listening address host:port").Default(":8278").String(),
	}

	store       = app.Command("store", "Start a vasto store")
	storeOption = &s.StoreOption{
		Dir:        store.Flag("dir", "folder to store data").Default(os.TempDir()).String(),
		Host:       store.Flag("host", "store host address").Default(util.GetLocalIP()).String(),
		ListenHost: store.Flag("listenHost", "store listening host address").Default("").String(),
		TcpPort:    store.Flag("tcpPort", "store listening tcp port").Default("8279").Int32(),
		GrpcPort:   store.Flag("grpcPort", "store listening grpc port").Default("8280").Int32(),
		UnixSocket: store.Flag("unixSocket", "store listening unix socket").Default("").Short('s').String(),
		Master:     store.Flag("master", "master address").Default("localhost:8278").String(),
		DataCenter: store.Flag("dataCenter", "data center name").Default("defaultDataCenter").String(),
	}
	storeProfile = store.Flag("cpuprofile", "cpu profile output file").Default("").String()

	gateway       = app.Command("gateway", "Start a vasto gateway")
	gatewayOption = &g.GatewayOption{
		Host:       gateway.Flag("host", "store listening host address.").Default("").String(),
		TcpPort:    gateway.Flag("tcpPort", "gateway listening port").Default("8282").Int32(),
		GrpcPort:   gateway.Flag("grpcPort", "store listening grpc port").Default("8283").Int32(),
		Master:     gateway.Flag("master", "master address").Default("localhost:8278").String(),
		DataCenter: gateway.Flag("dataCenter", "data center name").Default("defaultDataCenter").String(),
	}
	gatewayProfile = gateway.Flag("cpuprofile", "cpu profile output file").Default("").String()

	bench           = app.Command("bench", "Start a vasto benchmark")
	benchmarkOption = &b.BenchmarkOption{
		StoreAddress: bench.Flag("storeTcpAddress", "store listening tcp address").Default("localhost:8279").String(),
		UnixSocket:   bench.Flag("socket", "store listening unix socket").Default("").Short('s').String(),
		ClientCount:  bench.Flag("clientCount", "parallel client count").Default("8").Short('c').Int32(),
		RequestCount: bench.Flag("requestCount", "total request count").Default("1024000").Short('n').Int32(),
		Master:       bench.Flag("master", "master address").Default("localhost:8278").String(),
		DataCenter:   bench.Flag("dataCenter", "data center name").Default("defaultDataCenter").String(),
	}
)

func main() {

	cmd := kingpin.MustParse(app.Parse(os.Args[1:]))

	cpuProfile := *storeProfile + *gatewayProfile

	if cpuProfile != "" {
		println("profiling to", cpuProfile)

		f, err := os.Create(cpuProfile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
		on_interrupt.OnInterrupt(func() {
			pprof.StopCPUProfile()
		}, func() {
			pprof.StopCPUProfile()
		})
	}

	switch cmd {

	case master.FullCommand():
		m.RunMaster(masterOption)

	case store.FullCommand():
		s.RunStore(storeOption)

	case gateway.FullCommand():
		g.RunGateway(gatewayOption)

	case bench.FullCommand():
		b.RunBenchmarker(benchmarkOption)

	}
}
