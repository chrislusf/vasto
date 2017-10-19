package benchmark

import (
	"fmt"
	"log"
	"time"

	"github.com/chrislusf/vasto/cmd/client"
)

func (b *benchmarker) runBenchmarkerOnCluster(option *BenchmarkOption) {

	b.startThreads("put", func(hist *Histogram) {
		b.startVastoClient(hist, func(c *client.VastoClient, i int) error {

			key := []byte(fmt.Sprintf("k%5d", i))
			value := []byte(fmt.Sprintf("v%5d", i))

			return c.Put(key, value)
		})
	})

}

func (b *benchmarker) startVastoClient(hist *Histogram, fn func(c *client.VastoClient, i int) error) error {

	c := client.New(&client.ClientOption{
		Master:     b.option.Master,
		DataCenter: b.option.DataCenter,
	})

	clientReadyChan := make(chan bool)
	go c.Start(clientReadyChan)
	<-clientReadyChan

	requestCount := int(*b.option.RequestCount / *b.option.ClientCount)

	for i := 0; i < requestCount; i++ {
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
