package benchmark

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/chrislusf/vasto/vs"
	"github.com/gosuri/uiprogress"
)

// BenchmarkOption stores options for benchmarking
type BenchmarkOption struct {
	Master            *string
	DataCenter        *string
	Keyspace          *string
	ClientCount       *int32
	RequestCount      *int32
	RequestCountStart *int32
	BatchSize         *int32
	Tests             *string
	DisableUnixSocket *bool
}

type benchmarker struct {
	option *BenchmarkOption
}

// RunBenchmarker starts a benchmarking run
func RunBenchmarker(option *BenchmarkOption) {
	var b = &benchmarker{
		option: option,
	}

	println("benchmarking on cluster with master", *option.Master)
	b.runBenchmarkerOnCluster(context.Background(), option)
}

func (b *benchmarker) runBenchmarkerOnCluster(ctx context.Context, option *BenchmarkOption) {

	uiprogress.Start()

	for _, t := range strings.Split(*option.Tests, ",") {

		switch t {
		case "put":

			bar := uiprogress.AddBar(int(*option.RequestCount)).AppendCompleted().PrependElapsed()
			bar.PrependFunc(func(b *uiprogress.Bar) string {
				return "put : "
			})
			bar.AppendFunc(func(b *uiprogress.Bar) string {
				return fmt.Sprintf("%.2f ops/sec", float64(b.Current())/b.TimeElapsed().Seconds())
			})

			b.startThreadsWithClient(ctx, *option.Tests, func(hist *Histogram, c *vs.ClusterClient, start, stop, batchSize int) {
				b.execute(hist, c, start, stop, batchSize, func(c *vs.ClusterClient, i int) error {

					var rows []*vs.KeyValue

					for t := 0; t < batchSize; t++ {
						key := []byte(fmt.Sprintf("k%d", i+t))
						value := []byte(fmt.Sprintf("v%d", i+t))

						row := vs.NewKeyValue(key, value)

						rows = append(rows, row)

						bar.Incr()
					}

					err := c.BatchPut(rows)
					if err != nil {
						log.Printf("write %d rows, started with %v:  %v", len(rows), string(rows[0].KeyObject.GetKey()), err)
					}
					return err
				})
			})
		case "get":

			bar := uiprogress.AddBar(int(*option.RequestCount)).AppendCompleted().PrependElapsed()
			bar.PrependFunc(func(b *uiprogress.Bar) string {
				return "get : "
			})
			bar.AppendFunc(func(b *uiprogress.Bar) string {
				return fmt.Sprintf("%.2f ops/sec", float64(b.Current())/b.TimeElapsed().Seconds())
			})

			b.startThreadsWithClient(ctx, *option.Tests, func(hist *Histogram, c *vs.ClusterClient, start, stop, batchSize int) {
				b.execute(hist, c, start, stop, batchSize, func(c *vs.ClusterClient, i int) error {

					if batchSize == 1 {

						key := vs.Key([]byte(fmt.Sprintf("k%d", i)))
						value := []byte(fmt.Sprintf("v%d", i))

						data, _, err := c.Get(key)

						if err != nil {
							log.Printf("read %s: %v", string(key.GetKey()), err)
							return err
						}
						if bytes.Compare(data, value) != 0 {
							log.Printf("read %s, expected %s", string(data), string(value))
							return nil
						}

						bar.Incr()

						return nil

					}

					var keys []*vs.KeyObject
					for t := 0; t < batchSize; t++ {
						key := []byte(fmt.Sprintf("k%d", i+t))
						keys = append(keys, vs.Key(key))
					}

					data, err := c.BatchGet(keys)

					if err != nil {
						log.Printf("batch read %d keys: %v", len(keys), err)
						return err
					}
					for _, kv := range data {
						if kv == nil {
							continue
						}
						if len(kv.GetKey()) <= 1 || len(kv.GetValue()) <= 1 {
							log.Printf("read strange kv %s:%s", string(kv.GetKey()), string(kv.GetValue()))
							continue
						}
						if bytes.Compare(kv.GetKey()[1:], kv.GetValue()[1:]) != 0 {
							log.Printf("read unexpected %s:%s", string(kv.GetKey()), string(kv.GetValue()))
							return nil
						}
						bar.Incr()
					}

					return nil

				})
			})
		}
	}

	uiprogress.Stop()

}

func (b *benchmarker) startThreadsWithClient(ctx context.Context, name string, fn func(hist *Histogram, c *vs.ClusterClient, start, stop, batchSize int)) {

	requestCountEachClient := int(*b.option.RequestCount / *b.option.ClientCount)

	b.startThreads(name, requestCountEachClient, int(*b.option.RequestCountStart), func(hist *Histogram, start, stop, batchSize int) {
		vc := vs.NewVastoClient(ctx, "benchmarker", *b.option.Master, *b.option.DataCenter)
		if *b.option.DisableUnixSocket {
			vc.ClusterListener.SetUnixSocket(false)
		}
		fn(hist, vc.NewClusterClient(*b.option.Keyspace), start, stop, batchSize)
	})

}

func (b *benchmarker) execute(hist *Histogram, c *vs.ClusterClient, start, stop, batchSize int, fn func(c *vs.ClusterClient, i int) error) error {

	for i := start; i < stop; i += batchSize {
		start := time.Now()
		err := fn(c, i)
		if err != nil {
			log.Printf("benchmark error: %v", err)
			return err
		}
		taken := float64(time.Since(start) / 1000)
		hist.Add(taken)
	}

	return nil
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
