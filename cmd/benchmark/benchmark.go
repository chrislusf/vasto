package benchmark

import (
	"fmt"
	"sync"
	"time"
)

type BenchmarkOption struct {
	// store mode options
	StoreAddress *string
	UnixSocket   *string
	// fixed cluster mode options
	FixedCluster *string
	// dynamic cluster mode options
	Master     *string
	DataCenter *string
	// detail options
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
	if *option.Master != "" {
		b.runBenchmarkerOnCluster(option)
	} else {
		b.runBenchmarkerOnStore(option)
	}
}

func (b *benchmarker) startThreads(name string, fn func(hist *Histogram)) {
	start := time.Now()
	clientCount := int(*b.option.ClientCount)

	hists := make([]Histogram, clientCount)
	var wg sync.WaitGroup
	for i := 0; i < clientCount; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			fn(&hists[i])
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
