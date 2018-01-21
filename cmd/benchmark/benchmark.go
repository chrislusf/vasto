package benchmark

import (
	"fmt"
	"sync"
	"time"
	"context"

	"github.com/chrislusf/vasto/client"
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
	ClientCount       *int32
	RequestCount      *int32
	RequestCountStart *int32
	BatchSize         *int32
	Tests             *string
}

type benchmarker struct {
	option *BenchmarkOption
}

func RunBenchmarker(option *BenchmarkOption) {
	var b = &benchmarker{
		option: option,
	}

	println("benchmarking on cluster with master", *option.Master)
	b.runBenchmarkerOnCluster(context.Background(), option)
}

func (b *benchmarker) startThreads(name string, requestCountEachClient int, requestNumberStart int, fn func(hist *Histogram, start, stop, batchSize int)) {
	start := time.Now()
	clientCount := int(*b.option.ClientCount)

	hists := make([]Histogram, clientCount)
	var wg sync.WaitGroup
	for i := 0; i < clientCount; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			fn(&hists[i], i*requestCountEachClient+requestNumberStart, (i+1)*requestCountEachClient+requestNumberStart, int(*b.option.BatchSize))
		}(i)
	}
	wg.Wait()

	elapsed := time.Since(start)
	speed := float64(*b.option.RequestCount) / float64(elapsed.Seconds())
	fmt.Printf("%-10s : %9.1f op/s\n", name, speed)

	var hist = hists[0]
	for i := 1; i < len(hists); i++ {
		hist.Merge(&hists[i])
	}
	fmt.Printf("Microseconds per op:\n%s\n", hist.ToString())
}

func (b *benchmarker) startThreadsWithClient(ctx context.Context, name string, fn func(hist *Histogram, c *client.VastoClient, start, stop, batchSize int)) {

	requestCountEachClient := int(*b.option.RequestCount / *b.option.ClientCount)

	b.startThreads(name, requestCountEachClient, int(*b.option.RequestCountStart), func(hist *Histogram, start, stop, batchSize int) {
		c := client.NewClient(&client.ClientOption{
			FixedCluster: b.option.FixedCluster,
			Master:       b.option.Master,
			DataCenter:   b.option.DataCenter,
			Keyspace:     b.option.Keyspace,
			ClientName:   "benchmarker",
		})
		c.StartClient(ctx)
		fn(hist, c, start, stop, batchSize)
	})

}
