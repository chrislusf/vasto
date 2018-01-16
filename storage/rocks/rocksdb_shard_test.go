package rocks

import (
	"fmt"
	"testing"

	"github.com/chrislusf/gorocksdb"
	"math"
)

func TestSetCompactionForShard(t *testing.T) {

	db := setupTestDb()
	defer cleanup(db)

	total := 100000
	shardCount := 5

	for i := 0; i < total; i++ {
		key := []byte(fmt.Sprintf("k%5d", i))
		value := []byte(fmt.Sprintf("v%5d", i))
		db.Put(key, value)
	}

	db.SetCompactionForShard(0, shardCount)
	db.db.CompactRange(gorocksdb.Range{nil, nil})

	var counter4 = count(db)
	expected := float64(total) / float64(shardCount)
	if math.Abs(float64(counter4)-expected) > expected*0.01 {
		t.Errorf("scanning expecting %d rows, but actual %d rows", 50000, counter4)
	}
	fmt.Printf("sharded to %d, expecting %.2f\n", counter4, expected)

}
