package benchmark

import (
	"fmt"
	"log"
	"time"

	"bytes"
	"github.com/chrislusf/vasto/cmd/client"
	"strings"
	"context"
	"github.com/gosuri/uiprogress"
)

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

					return c.Put(*b.option.Keyspace, rows)
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

					for t := 0; t < batchSize; t++ {
						key := []byte(fmt.Sprintf("k%d", i+t))
						value := []byte(fmt.Sprintf("v%d", i+t))

						data, err := c.Get(*b.option.Keyspace, key)

						bar.Incr()

						if err != nil {
							log.Printf("read %s: %v", string(key), err)
							return err
						}
						if bytes.Compare(data, value) != 0 {
							log.Printf("read %s, expected %s", string(data), string(value))
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

func (b *benchmarker) execute(hist *Histogram, c *client.VastoClient, start, stop, batchSize int, fn func(c *client.VastoClient, i int) error) error {

	for i := start; i < stop; i += batchSize {
		start := time.Now()
		err := fn(c, i)
		if err != nil {
			log.Printf("put error: %v", err)
			return err
		}
		taken := float64(time.Since(start) / 1000)
		hist.Add(taken)
	}

	return nil
}
