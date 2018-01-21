package benchmark

import (
	"fmt"
	"sync"
	"time"
	"context"
	"log"
	"bytes"
	"strings"

	"github.com/chrislusf/vasto/client"
	"github.com/gosuri/uiprogress"
)

type BenchmarkOption struct {
	// store mode options
	StoreAddress      *string
	DisableUnixSocket *bool
	Master            *string
	DataCenter        *string
	Keyspace          *string
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

			b.startThreadsWithClient(ctx, *option.Tests, func(hist *Histogram, c *client.VastoClient, start, stop, batchSize int) {
				b.execute(hist, c, start, stop, batchSize, func(c *client.VastoClient, i int) error {

					var rows []*client.Row

					for t := 0; t < batchSize; t++ {
						key := []byte(fmt.Sprintf("k%d", i+t))
						value := []byte(fmt.Sprintf("v%d", i+t))

						row := client.NewRow(key, value)

						rows = append(rows, row)

						bar.Incr()
					}

					err := c.Put(*b.option.Keyspace, rows)
					if err != nil {
						log.Printf("write %d rows, started with %v:  %v", len(rows), string(rows[0].Key), err)
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

			b.startThreadsWithClient(ctx, *option.Tests, func(hist *Histogram, c *client.VastoClient, start, stop, batchSize int) {
				b.execute(hist, c, start, stop, batchSize, func(c *client.VastoClient, i int) error {

					if batchSize == 1 {

						key := []byte(fmt.Sprintf("k%d", i))
						value := []byte(fmt.Sprintf("v%d", i))

						data, err := c.Get(*b.option.Keyspace, key)

						if err != nil {
							log.Printf("read %s: %v", string(key), err)
							return err
						}
						if bytes.Compare(data, value) != 0 {
							log.Printf("read %s, expected %s", string(data), string(value))
							return nil
						}

						bar.Incr()

						return nil

					}

					var keys [][]byte
					for t := 0; t < batchSize; t++ {
						key := []byte(fmt.Sprintf("k%d", i+t))
						keys = append(keys, key)
					}

					data, err := c.BatchGet(*b.option.Keyspace, keys)

					if err != nil {
						log.Printf("batch read %d keys: %v", len(keys), err)
						return err
					}
					for _, kv := range data {
						if kv == nil {
							continue
						}
						if len(kv.Key) <= 1 || len(kv.Value) <= 1 {
							log.Printf("read strange kv %s:%s", string(kv.Key), string(kv.Value))
							continue
						}
						if bytes.Compare(kv.Key[1:], kv.Value[1:]) != 0 {
							log.Printf("read unexpected %s:%s", string(kv.Key), string(kv.Value))
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

func (b *benchmarker) startThreadsWithClient(ctx context.Context, name string, fn func(hist *Histogram, c *client.VastoClient, start, stop, batchSize int)) {

	requestCountEachClient := int(*b.option.RequestCount / *b.option.ClientCount)

	b.startThreads(name, requestCountEachClient, int(*b.option.RequestCountStart), func(hist *Histogram, start, stop, batchSize int) {
		c := client.NewClient(&client.ClientOption{
			Master:     b.option.Master,
			DataCenter: b.option.DataCenter,
			Keyspace:   b.option.Keyspace,
			ClientName: "benchmarker",
		})
		c.StartClient(ctx)
		fn(hist, c, start, stop, batchSize)
	})

}

func (b *benchmarker) execute(hist *Histogram, c *client.VastoClient, start, stop, batchSize int, fn func(c *client.VastoClient, i int) error) error {

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
