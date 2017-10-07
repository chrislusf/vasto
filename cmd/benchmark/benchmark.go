package benchmark

import (
	"fmt"
	"github.com/chrislusf/vasto/pb"
	"log"
	"math/rand"
	"net"
	"sync"
	"time"
)

type BenchmarkOption struct {
	StoreAddress *string
	UnixSocket   *string
	Master       *string
	DataCenter   *string
	ClientCount  *int32
	RequestCount *int32
	BatchSize    *int32
}

type benchmarker struct {
	option *BenchmarkOption
}

func RunBenchmarker(option *BenchmarkOption) {
	var b = &benchmarker{
		option: option,
	}

	b.startThreads("put", func(r *rand.Rand) *pb.Requests {
		requests := &pb.Requests{}
		for i := 0; i < int(*b.option.BatchSize); i++ {
			request := &pb.Request{
				Put: &pb.PutRequest{
					KeyValue: &pb.KeyValue{
						Key:   make([]byte, 4),
						Value: make([]byte, 4),
					},
					TimestampNs: 0,
					TtlMs:       0,
				},
			}
			Uint32toBytes(request.Put.KeyValue.Key, r.Uint32())
			Uint32toBytes(request.Put.KeyValue.Value, r.Uint32())
			requests.Requests = append(requests.Requests, request)
		}
		return requests
	})
}

func (b *benchmarker) startThreads(name string, op operation) {
	start := time.Now()
	clientCount := int(*b.option.ClientCount)

	hists := make([]Histogram, clientCount)
	var wg sync.WaitGroup
	for i := 0; i < clientCount; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			b.startClient(&hists[i], op)
		}(i)
	}
	wg.Wait()

	elapsed := time.Since(start)
	speed := float64(*b.option.RequestCount**b.option.BatchSize) * 1e9 / float64(elapsed)
	fmt.Printf("%-10s : %9.1f op/s\n", name, speed)

	var hist = hists[0]
	for i := 1; i < len(hists); i++ {
		hist.Merge(&hists[i])
	}
	fmt.Printf("Microseconds per op:\n%s\n", hist.ToString())
}

func (b *benchmarker) startClient(hist *Histogram, op operation) error {

	network, address := "tcp", *b.option.StoreAddress
	if *b.option.UnixSocket != "" {
		network, address = "unix", *b.option.UnixSocket
	}

	conn, err := net.Dial(network, address)
	if err != nil {
		return fmt.Errorf("dial store %s %s: %v", network, address, err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Time{})
	if c, ok := conn.(*net.TCPConn); ok {
		c.SetKeepAlive(true)
		c.SetNoDelay(true)
	}

	requestCount := int(*b.option.RequestCount / *b.option.ClientCount)
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	for i := 0; i < requestCount; i++ {
		start := time.Now()
		responses, err := pb.SendRequest(conn, op(r))
		if err != nil {
			log.Printf("put error: %v", err)
			return err
		}
		taken := float64(time.Since(start) / 1000)
		for range responses.Responses {
			hist.Add(taken)
		}
	}

	return nil
}

type operation func(rand *rand.Rand) *pb.Requests

func Uint32toBytes(b []byte, v uint32) {
	for i := uint(0); i < 4; i++ {
		b[3-i] = byte(v >> (i * 8))
	}
}
