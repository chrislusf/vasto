package benchmark

import (
	"fmt"
	"github.com/chrislusf/vasto/cmd/client"
	"sync"
	"time"
	"context"
)

type BenchmarkOption struct {
	// store mode options
	StoreAddress      *string
	DisableUnixSocket *bool
	// fixed cluster mode options
	FixedCluster *string
	// dynamic cluster mode options
	Master     *string
	DataCenter *string
	Keyspace   *string
	// detail options
	ClientCount  *int32
	RequestCount *int32
	BatchSize    *int32
	Tests        *string
}

type benchmarker struct {
	option *BenchmarkOption
}

func RunBenchmarker(option *BenchmarkOption) {
	var b = &benchmarker{
		option: option,
	}

	if *option.Master != "" {
		println("benchmarking on cluster with master", *option.Master)
		b.runBenchmarkerOnCluster(context.Background(), option)
	} else {
		println("benchmarking on single store or gateway", *option.StoreAddress)
		b.runBenchmarkerOnStore(option)
	}
}

func (b *benchmarker) startThreads(name string, requestCount int, fn func(hist *Histogram, start, stop, batchSize int)) {
	start := time.Now()
	clientCount := int(*b.option.ClientCount)

	hists := make([]Histogram, clientCount)
	var wg sync.WaitGroup
	for i := 0; i < clientCount; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			fn(&hists[i], i*requestCount, (i+1)*requestCount, int(*b.option.BatchSize))
		}(i)
	}
	wg.Wait()

	elapsed := time.Since(start)
	speed := float64(*b.option.RequestCount * *b.option.BatchSize) * 1e9 / float64(elapsed)
	fmt.Printf("%-10s : %9.1f op/s\n", name, speed)

	var hist = hists[0]
	for i := 1; i < len(hists); i++ {
		hist.Merge(&hists[i])
	}
	fmt.Printf("Microseconds per op:\n%s\n", hist.ToString())
}

func (b *benchmarker) startThreadsWithClient(ctx context.Context, name string, fn func(hist *Histogram, c *client.VastoClient, start, stop, batchSize int)) {

	requestCount := int(*b.option.RequestCount / *b.option.ClientCount)

	b.startThreads(name, requestCount, func(hist *Histogram, start, stop, batchSize int) {
		c := client.NewClient(&client.ClientOption{
			FixedCluster: b.option.FixedCluster,
			Master:       b.option.Master,
			DataCenter:   b.option.DataCenter,
			Keyspace:     b.option.Keyspace,
		})
		c.StartClient(ctx)
		fn(hist, c, start, stop, batchSize)
	})

}
