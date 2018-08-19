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
	"os"
	"runtime/pprof"

	"flag"
	"github.com/chrislusf/glog"
	a "github.com/chrislusf/vasto/cmd/admin"
	b "github.com/chrislusf/vasto/cmd/benchmark"
	g "github.com/chrislusf/vasto/cmd/gateway"
	m "github.com/chrislusf/vasto/cmd/master"
	sh "github.com/chrislusf/vasto/cmd/shell"
	s "github.com/chrislusf/vasto/cmd/store"
	"github.com/chrislusf/vasto/util"
	"github.com/chrislusf/vasto/util/interrupt"
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
		DisableUnixSocket: store.Flag("disableUnixSocket", "store listening unix socket").Default("false").Bool(),
		Master:            store.Flag("master", "master address").Default("localhost:8278").String(),
		DataCenter:        store.Flag("dataCenter", "data center name").Default("dc1").String(),
		LogFileSizeMb:     store.Flag("logFileSizeMb", "log file size limit in MB").Default("128").Int(),
		LogFileCount:      store.Flag("logFileCount", "log file count limit").Default("3").Int(),
		DiskSizeGb:        store.Flag("diskSizeGb", "disk size in GB").Default("10").Int(),
		Tags:              store.Flag("tags", "comma separated tags").Default("").String(),
		DisableBinLog:     store.Flag("disableBinLog", "disable binary log").Default("false").Bool(),
	}
	storeProfile = store.Flag("cpuprofile", "cpu profile output file").Default("").String()

	server             = app.Command("server", "Start a vasto master and a vasto store")
	serverMasterOption = &m.MasterOption{
		Address: server.Flag("master.address", "listening address host:port").Default(":8278").String(),
	}
	serverStoreOption = &s.StoreOption{
		Dir:               server.Flag("store.dir", "folder to server data").Default(os.TempDir()).String(),
		Host:              server.Flag("store.host", "server host address").Default(util.GetLocalIP()).String(),
		ListenHost:        server.Flag("store.listenHost", "server listening host address").Default("").String(),
		TcpPort:           server.Flag("store.port", "server listening tcp port").Default("8279").Int32(),
		DisableUnixSocket: server.Flag("store.disableUnixSocket", "server listening unix socket").Default("false").Bool(),
		Master:            server.Flag("store.master", "master address").Default("localhost:8278").String(),
		DataCenter:        server.Flag("store.dataCenter", "data center name").Default("dc1").String(),
		LogFileSizeMb:     server.Flag("store.logFileSizeMb", "log file size limit in MB").Default("128").Int(),
		LogFileCount:      server.Flag("store.logFileCount", "log file count limit").Default("3").Int(),
		DiskSizeGb:        server.Flag("store.diskSizeGb", "disk size in GB").Default("10").Int(),
		Tags:              server.Flag("store.tags", "comma separated tags").Default("").String(),
	}
	serverProfile = server.Flag("cpuprofile", "cpu profile output file").Default("").String()

	gateway       = app.Command("gateway", "Start a vasto gateway")
	gatewayOption = &g.GatewayOption{
		TcpAddress: gateway.Flag("address", "gateway tcp host address").Default(":8281").String(),
		UnixSocket: gateway.Flag("unixSocket", "gateway listening unix socket").Default("").Short('s').String(),
		Master:     gateway.Flag("master", "master address").Default("localhost:8278").String(),
		DataCenter: gateway.Flag("dataCenter", "data center name").Default("dc1").String(),
		Keyspace:   gateway.Flag("keyspace", "keyspace name").Default("").String(),
	}
	gatewayProfile = gateway.Flag("cpuprofile", "cpu profile output file").Default("").String()

	bench           = app.Command("bench", "Start a vasto benchmark")
	benchmarkOption = &b.BenchmarkOption{
		ClientCount:       bench.Flag("clientCount", "parallel client count").Default("2").Short('c').Int32(),
		RequestCount:      bench.Flag("requestCount", "total request count").Default("1024000").Short('n').Int32(),
		RequestCountStart: bench.Flag("requestNumberStart", "starting request index").Default("0").Int32(),
		BatchSize:         bench.Flag("batchSize", "put requests in batch").Default("1").Short('b').Int32(),
		Master:            bench.Flag("master", "master address").Default("localhost:8278").String(),
		DataCenter:        bench.Flag("dataCenter", "data center name").Default("dc1").String(),
		Keyspace:          bench.Flag("keyspace", "keyspace name").Default("benchmark").String(),
		Tests:             bench.Flag("tests", "[put|get]").Default("put,get").Short('t').String(),
		DisableUnixSocket: bench.Flag("disableUnixSocket", "avoid unix socket and only use tcp network").Default("false").Bool(),
	}
	benchProfile = bench.Flag("cpuprofile", "cpu profile output file").Default("").String()

	shell       = app.Command("shell", "Start a vasto shell")
	shellOption = &sh.ShellOption{
		Master:     shell.Flag("master", "master address").Default("localhost:8278").String(),
		DataCenter: shell.Flag("dataCenter", "data center name").Default("dc1").String(),
		Keyspace:   shell.Flag("keyspace", "keyspace name").Default("").String(),
	}

	admin       = app.Command("admin", "Manage FixedCluster Size")
	adminOption = &a.AdminOption{
		Master: admin.Flag("master", "master address").Default("localhost:8278").String(),
	}
)

func main() {

	flag.Parse()

	cmd := kingpin.MustParse(app.Parse(flag.Args()))

	cpuProfile := *storeProfile + *gatewayProfile + *benchProfile

	if cpuProfile != "" {
		println("profiling to", cpuProfile)

		f, err := os.Create(cpuProfile)
		if err != nil {
			glog.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
		interrupt.OnInterrupt(func() {
			pprof.StopCPUProfile()
		}, func() {
			pprof.StopCPUProfile()
		})
	}

	*storeOption.Dir = fixHomeDir(*storeOption.Dir)
	*serverStoreOption.Dir = fixHomeDir(*serverStoreOption.Dir)

	switch cmd {

	case master.FullCommand():
		m.RunMaster(masterOption)

	case store.FullCommand():
		s.RunStore(storeOption)

	case server.FullCommand():
		go m.RunMaster(serverMasterOption)
		s.RunStore(serverStoreOption)

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

func fixHomeDir(dir string) string {
	if strings.HasPrefix(dir, "~") {
		usr, err := user.Current()
		if err != nil {
			glog.Fatal(err)
		}
		dir = usr.HomeDir + dir[1:]
	}
	return dir
}
