package topology

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"math"
	"testing"
	"github.com/chrislusf/vasto/pb"
)

// tests the GetAddress function on the Node interface
func TestNode(t *testing.T) {
	host := "localhost:7000"
	n := NewNode(3, &pb.StoreResource{
		DataCenter:"dc1",
		Network: "tcp",
		Address:host,
	})

	// validate address name
	assert.Equal(t, host, n.GetAddress())
	assert.Equal(t, 3, n.GetId())
}

// closure function for benchmarking multiple clusters
func baselineBenchmark(hosts int) func(b *testing.B) {
	ring := NewHashRing("ks1", "dc1", hosts, 2)
	for i := 0; i < hosts; i++ {
		n := NewNode(i, &pb.StoreResource{
			DataCenter:"dc1",
			Network: "tcp",
			Address:fmt.Sprint("localhost:", 7000+i),
			AdminAddress:fmt.Sprint("localhost:", 8000+i),
		})
		ring.Add(n)
	}

	return func(b *testing.B) {
		// use the ring hash a number
		for n := 0; n < b.N; n++ {
			ring.FindBucket(uint64(n))
		}
	}
}

// 5 Nodes
func Benchmark_5_NodeHashRing(b *testing.B) {
	baselineBenchmark(5)(b)
}

func Benchmark_25_NodeHashRing(b *testing.B) {
	baselineBenchmark(25)(b)
}

func Benchmark_100_NodeHashRing(b *testing.B) {
	baselineBenchmark(100)(b)
}

func Benchmark_1000_NodeHashRing(b *testing.B) {
	baselineBenchmark(1000)(b)
}

func TestHashing(t *testing.T) {
	ring1 := createRing(15)
	ring2 := createRing(16)

	var count = 500000
	var moved, movedInRing1 int
	ring1Size := ring1.CurrentSize()

	for n := 0; n < count; n++ {
		x := ring1.FindBucket(uint64(n))
		y := ring2.FindBucket(uint64(n))
		if x != y {
			moved += 1
			if y < ring1Size {
				movedInRing1 += 1
			}
		}
	}

	printMovedStats("moved", moved, count)
	printMovedStats("movedInRing1", movedInRing1, count)

	actualMovedPercentage := float64(100*moved) / float64(count)
	expectedMovePercentage := 100 * math.Abs(float64(ring2.CurrentSize()-ring1.CurrentSize())) / float64(ring2.CurrentSize())
	fmt.Printf("expected %f%%\n", expectedMovePercentage)

	assert.True(t, actualMovedPercentage < expectedMovePercentage+0.002)
}

func createRing(hosts int) *ClusterRing {
	ring := NewHashRing("ks1", "dc1", hosts, 2)
	for i := 0; i < hosts; i++ {
		n := NewNode(i, &pb.StoreResource{
			DataCenter:"dc1",
			Network: "tcp",
			Address:fmt.Sprint("localhost:", 7000+i),
			AdminAddress:fmt.Sprint("localhost:", 8000+i),
		})
		ring.Add(n)
	}
	return ring
}

func printMovedStats(name string, moved, count int) {
	actualMovedPercentage := float64(100*moved) / float64(count)

	fmt.Printf("%-10s: %d, percent: %f%%\n",
		name,
		moved,
		actualMovedPercentage,
	)
}
