package benchmark

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"time"

	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/util"
)

func (b *benchmarker) runBenchmarkerOnStore(option *BenchmarkOption) {

	b.startThreads("put", func(hist *Histogram) {
		b.startDirectClient(hist, func(r *rand.Rand) *pb.Requests {
			requests := &pb.Requests{}
			for i := 0; i < int(*b.option.BatchSize); i++ {
				request := &pb.Request{
					Put: &pb.PutRequest{
						KeyValue: &pb.KeyValue{
							Key:   make([]byte, 4),
							Value: make([]byte, 4),
						},
						TtlSecond: 0,
					},
				}
				Uint32toBytes(request.Put.KeyValue.Key, r.Uint32())
				Uint32toBytes(request.Put.KeyValue.Value, r.Uint32())
				request.Put.PartitionHash = util.Hash(request.Put.KeyValue.Key)
				requests.Requests = append(requests.Requests, request)
			}
			return requests
		})
	})

}

func (b *benchmarker) startDirectClient(hist *Histogram, op func(rand *rand.Rand) *pb.Requests) error {

	network, address := "tcp", *b.option.StoreAddress
	if *b.option.StoreUnixSocket != "" {
		network, address = "unix", *b.option.StoreUnixSocket
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
		responses, err := pb.SendRequests(conn, op(r))
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

func Uint32toBytes(b []byte, v uint32) {
	for i := uint(0); i < 4; i++ {
		b[3-i] = byte(v >> (i * 8))
	}
}
