package topology

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"math"
	"testing"
)

// tests the GetHost function on the Node interface
func TestNode(t *testing.T) {
	host := "localhost:7000"
	n := NewNode(3, host)

	// validate host name
	assert.Equal(t, host, n.GetHost())
	assert.Equal(t, 3, n.GetId())
}

// closure function for benchmarking multiple clusters
func baselineBenchmark(hosts int) func(b *testing.B) {
	ring := NewHashRing()
	for i := 0; i < hosts; i++ {
		ring.Add(NewNode(i, fmt.Sprint("localhost:", 7000+i)))
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
	var moved int

	for n := 0; n < count; n++ {
		x := ring1.FindBucket(uint64(n))
		y := ring2.FindBucket(uint64(n))
		if x != y {
			moved += 1
		}
	}

	actualMovedPercentage := float64(100*moved) / float64(count)
	expectedMovePercentage := 100 * math.Abs(float64(ring2.Size()-ring1.Size())) / float64(ring2.Size())

	fmt.Printf("moved: %d, percent: %f%% expected %f%%\n",
		moved,
		actualMovedPercentage,
		expectedMovePercentage,
	)

	assert.True(t, actualMovedPercentage < expectedMovePercentage+0.002)
}

func createRing(hosts int) Ring {
	ring := NewHashRing()
	for i := 0; i < hosts; i++ {
		ring.Add(NewNode(i, fmt.Sprint("localhost:", 7000+i)))
	}
	return ring
}
