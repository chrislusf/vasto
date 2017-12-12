package benchmark

import (
	"fmt"
	"log"
	"time"

	"bytes"
	"github.com/chrislusf/vasto/cmd/client"
	"strings"
	"context"
)

func (b *benchmarker) runBenchmarkerOnCluster(ctx context.Context, option *BenchmarkOption) {

	b.startThreadsWithClient(ctx, *option.Tests, func(hist *Histogram, c *client.VastoClient, start, stop int) {
		for _, t := range strings.Split(*option.Tests, ",") {
			switch t {
			case "put":
				b.execute(hist, c, start, stop, func(c *client.VastoClient, i int) error {

					key := []byte(fmt.Sprintf("k%5d", i))
					value := []byte(fmt.Sprintf("v%5d", i))

					return c.Put(*b.option.Keyspace, nil, key, value)
				})
			case "get":
				b.execute(hist, c, start, stop, func(c *client.VastoClient, i int) error {

					key := []byte(fmt.Sprintf("k%5d", i))
					value := []byte(fmt.Sprintf("v%5d", i))

					data, err := c.Get(*b.option.Keyspace, key)
					if err != nil {
						log.Printf("read %s: %v", string(key), err)
						return err
					}
					if bytes.Compare(data, value) != 0 {
						log.Printf("read %s, expected %s", string(data), string(value))
						return nil
					}

					return nil

				})
			}
		}
	})

}

func (b *benchmarker) execute(hist *Histogram, c *client.VastoClient, start, stop int, fn func(c *client.VastoClient, i int) error) error {

	for i := start; i < stop; i++ {
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
