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

	a "github.com/chrislusf/vasto/cmd/admin"
	b "github.com/chrislusf/vasto/cmd/benchmark"
	g "github.com/chrislusf/vasto/cmd/gateway"
	m "github.com/chrislusf/vasto/cmd/master"
	sh "github.com/chrislusf/vasto/cmd/shell"
	s "github.com/chrislusf/vasto/cmd/store"
	"github.com/chrislusf/vasto/util"
	"github.com/chrislusf/vasto/util/on_interrupt"
	"gopkg.in/alecthomas/kingpin.v2"
	"os/user"
	"strings"
)

var (
	app = kingpin.New("vasto", "a distributed fast key-value store")

	master       = app.Command("master", "Start a master process")
	masterOption = &m.MasterOption{
		Address: master.Flag("address", "listening address host:port").Default(":8278").String(),
	}

	store       = app.Command("store", "Start a vasto store")
	storeOption = &s.StoreOption{
		Dir:               store.Flag("dir", "folder to store data").Default(os.TempDir()).String(),
		Host:              store.Flag("host", "store host address").Default(util.GetLocalIP()).String(),
		ListenHost:        store.Flag("listenHost", "store listening host address").Default("").String(),
		TcpPort:           store.Flag("port", "store listening tcp port").Default("8279").Int32(),
		AdminPort:         store.Flag("adminPort", "store listening grpc port, default to tcp port + 10000").Default("0").Int32(),
		Bootstrap:         store.Flag("clearAndBootstrap", "clear local data and copy snapshot from other peers").Default("false").Bool(),
		DisableUnixSocket: store.Flag("disableUnixSocket", "store listening unix socket").Default("false").Bool(),
		Master:            store.Flag("master", "master address").Default("localhost:8278").String(),
		DataCenter:        store.Flag("dataCenter", "data center name").Default("defaultDataCenter").String(),
		LogFileSizeMb:     store.Flag("logFileSizeMb", "log file size limit in MB").Default("128").Int(),
		LogFileCount:      store.Flag("logFileCount", "log file count limit").Default("3").Int(),
		ReplicationFactor: store.Flag("replicationFactor", "number of physical copies").Default("3").Int(),
		DiskSizeGb:        store.Flag("diskSizeGb", "disk size in GB").Default("10").Int(),
		Tags:              store.Flag("tags", "comma separated tags").Default("").String(),
		DisableUseEventIo: store.Flag("disableUseEventIo", "use event loop for network").Default("false").Bool(),
		FixedCluster:      store.Flag("fixed.cluster", "overwrite --master, format network:host:port[,network:host:port]*").Default("").String(),
		Keyspace:          store.Flag("fixed.keyspace", "keyspace name").Default("keyspace1").String(),
	}
	storeProfile = store.Flag("cpuprofile", "cpu profile output file").Default("").String()

	gateway       = app.Command("gateway", "Start a vasto gateway")
	gatewayOption = &g.GatewayOption{
		TcpAddress: gateway.Flag("address", "gateway tcp host address").Default(":8281").String(),
		UnixSocket: gateway.Flag("unixSocket", "gateway listening unix socket").Default("").Short('s').String(),
		FixedCluster: gateway.Flag("cluster.fixed",
			"overwrite --master, format network:host:port[,network:host:port]*").Default("").String(),
		Master:     gateway.Flag("master", "master address").Default("localhost:8278").String(),
		DataCenter: gateway.Flag("dataCenter", "data center name").Default("defaultDataCenter").String(),
		Keyspace:   gateway.Flag("keyspace", "keyspace name").Default("").String(),
	}
	gatewayProfile = gateway.Flag("cpuprofile", "cpu profile output file").Default("").String()

	bench           = app.Command("bench", "Start a vasto benchmark")
	benchmarkOption = &b.BenchmarkOption{
		StoreAddress:      bench.Flag("store.tcpAddress", "store listening tcp address").Default("localhost:8279").String(),
		DisableUnixSocket: bench.Flag("disableUnixSocket", "store listening unix socket").Default("false").Bool(),
		ClientCount:       bench.Flag("clientCount", "parallel client count").Default("2").Short('c').Int32(),
		RequestCount:      bench.Flag("requestCount", "total request count").Default("1024000").Short('n').Int32(),
		BatchSize:         bench.Flag("batchSize", "put requests in batch").Default("1").Short('b').Int32(),
		FixedCluster: bench.Flag("fixed.cluster",
			"overwrite --cluster.master, format network:host:port[,network:host:port]*").Default("").String(),
		Master:     bench.Flag("cluster.master", "master address").Default("localhost:8278").String(),
		DataCenter: bench.Flag("cluster.dataCenter", "data center name").Default("defaultDataCenter").String(),
		Keyspace:   bench.Flag("cluster.keyspace", "keyspace name").Default("benchmark").String(),
		Tests:      bench.Flag("tests", "[put|get]").Default("put,get").Short('t').String(),
	}

	shell       = app.Command("shell", "Start a vasto shell")
	shellOption = &sh.ShellOption{
		FixedCluster: shell.Flag("cluster.fixed",
			"overwrite --cluster.master, format network:host:port[,network:host:port]*").Default("").String(),
		Master:     shell.Flag("cluster.master", "master address").Default("localhost:8278").String(),
		DataCenter: shell.Flag("cluster.dataCenter", "data center name").Default("defaultDataCenter").String(),
		Keyspace:   shell.Flag("cluster.keyspace", "keyspace name").Default("").String(),
	}

	admin       = app.Command("admin", "Manage FixedCluster Size")
	adminOption = &a.AdminOption{
		Master: admin.Flag("master", "master address").Default("localhost:8278").String(),
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
		dir := *storeOption.Dir
		if strings.HasPrefix(dir, "~") {
			usr, err := user.Current()
			if err != nil {
				log.Fatal(err)
			}
			dir = usr.HomeDir + dir[1:]
		}
		*storeOption.Dir = dir
		s.RunStore(storeOption)

	case gateway.FullCommand():
		g.RunGateway(gatewayOption)

	case bench.FullCommand():
		b.RunBenchmarker(benchmarkOption)

	case shell.FullCommand():
		sh.RunShell(shellOption)

	case admin.FullCommand():
		a.RunAdmin(adminOption)

	}
}
