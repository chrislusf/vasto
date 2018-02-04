package rocks

import (
	"fmt"
	"testing"

	"github.com/chrislusf/gorocksdb"
	"math"
	"github.com/chrislusf/vasto/storage/codec"
	"github.com/chrislusf/vasto/util"
	"time"
)

func TestSetCompactionForShard(t *testing.T) {

	db := setupTestDb()
	defer cleanup(db)

	total := 100000
	shardCount := 5
	now := uint64(time.Now().Unix())

	for i := 0; i < total; i++ {
		key := []byte(fmt.Sprintf("k%5d", i))
		entry := &codec.Entry{
			PartitionHash:  util.Hash(key),
			UpdatedAtNs:    now,
			TtlSecond:      0,
			opsAndDataType: codec.BYTES,
			Value:          []byte(fmt.Sprintf("v%5d", i)),
		}
		db.Put(key, entry.ToBytes())
	}

	db.SetCompactionForShard(0, shardCount)
	db.db.CompactRange(gorocksdb.Range{nil, nil})

	var counter4 = count(db)
	expected := float64(total) / float64(shardCount)
	if math.Abs(float64(counter4)-expected) > expected*0.01 {
		t.Errorf("scanning expecting %d rows, but actual %d rows", int(expected), counter4)
	}
	fmt.Printf("sharded to %d, expecting %.2f\n", counter4, expected)

}
