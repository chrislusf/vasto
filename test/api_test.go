package test

import (
	"fmt"
	"net"
	"testing"

	"bytes"
	"github.com/chrislusf/vasto/client"
	m "github.com/chrislusf/vasto/cmd/master"
	s "github.com/chrislusf/vasto/cmd/store"
	"log"
	"time"
	"context"
	"os"
)

func TestOpen(t *testing.T) {

	masterPort := startMasterAndStore()

	time.Sleep(100 * time.Millisecond)

	c := client.NewClient(context.Background(), "[testing]", fmt.Sprintf("localhost:%d", masterPort), "dc1")

	c.CreateCluster("ks1", "dc1", 1, 1)

	log.Println("created keyspace ks1")

	ks := c.GetClusterClient("ks1")

	log.Println("GetClusterClient ks1")

	ks.BatchPut([]*client.Row{
		client.NewRow([]byte("x1"), []byte("y2")),
		client.NewRow([]byte("x2"), []byte("y2")),
		client.NewRow([]byte("x3"), []byte("y3")),
	})

	data, err := ks.Get([]byte("x2"))
	if err != nil {
		t.Errorf("fail to get value: %v", err)
	}
	if bytes.Compare(data, []byte("y2")) != 0 {
		t.Errorf("get: %v, expecting: %v", data, []byte("y2"))
	}

	t.Run("add", func(t *testing.T) {
		k := client.Key([]byte("y1"))

		ks.AddFloat64(k, 1)
		ks.AddFloat64(k, 1)
		ks.AddFloat64(k, 1)

		x, _ := ks.GetFloat64(k)

		if x != 3 {
			t.Errorf("get float64: %f, expecting: %v", x, 3)
		}
	})

	t.Run("max", func(t *testing.T) {
		k := client.Key([]byte("max1"))
		ks.MaxFloat64(k, 1)
		x, _ := ks.GetFloat64(k)
		if x != 1 {
			t.Errorf("get max float64: %f, expecting: %v", x, 1)
		}
		ks.MaxFloat64(k, 100)
		x, _ = ks.GetFloat64(k)
		if x != 100 {
			t.Errorf("get max float64: %f, expecting: %v", x, 100)
		}
		ks.MaxFloat64(k, 50)
		x, _ = ks.GetFloat64(k)
		if x != 100 {
			t.Errorf("get max float64: %f, expecting: %v", x, 100)
		}
	})

	t.Run("min", func(t *testing.T) {
		k := client.Key([]byte("min1"))
		ks.MinFloat64(k, 50)
		x, _ := ks.GetFloat64(k)
		if x != 50 {
			t.Errorf("get min float64: %f, expecting: %v", x, 50)
		}
		ks.MinFloat64(k, 100)
		x, _ = ks.GetFloat64(k)
		if x != 50 {
			t.Errorf("get min float64: %f, expecting: %v", x, 50)
		}
		ks.MinFloat64(k, 1)
		x, _ = ks.GetFloat64(k)
		if x != 1 {
			t.Errorf("get min float64: %f, expecting: %v", x, 1)
		}
	})

	os.RemoveAll("./ks1")
}

func startMasterAndStore() int {

	masterPort := getPort()

	go m.RunMaster(&m.MasterOption{
		Address: getString(fmt.Sprintf(":%d", masterPort)),
	})

	storeOption := &s.StoreOption{
		Dir:               getString("."),
		Host:              getString("localhost"),
		ListenHost:        getString(""),
		TcpPort:           getInt32(getPort()),
		Bootstrap:         getBool(true),
		DisableUnixSocket: getBool(false),
		Master:            getString(fmt.Sprintf("localhost:%d", masterPort)),
		DataCenter:        getString("dc1"),
		LogFileSizeMb:     getInt(128),
		LogFileCount:      getInt(3),
		DiskSizeGb:        getInt(10),
		Tags:              getString(""),
		DisableUseEventIo: getBool(false),
	}

	go s.RunStore(storeOption)

	return masterPort

}

func getFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port - 10000, nil
}

func getPort() int {
	port, err := getFreePort()
	if err != nil {
		log.Fatalf("can not get port: %v", err)
	}
	return port
}

func getString(x string) *string {
	return &x
}
func getInt32(x int) *int32 {
	y := int32(x)
	return &y
}
func getBool(x bool) *bool {
	return &x
}
func getInt(x int) *int {
	return &x
}
