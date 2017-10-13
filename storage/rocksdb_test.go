package storage

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func TestPutGet(t *testing.T) {
	db := newDB()
	db.setup("/tmp/rocks-test-go")
	defer func() {
		db.Close()
		db.Destroy()
	}()

	key := make([]byte, 4)
	value := make([]byte, 4)
	rand.Read(key)
	rand.Read(value)

	if err := db.Put(key, value); err != nil {
		t.Errorf("insert should not return any error. err: %v", err)
	}

	returned, err := db.Get(key)
	if err != nil {
		t.Errorf("get should not return any error. err: %v", err)
	}

	if !bytes.Equal(value, returned) {
		t.Errorf("data value is different. is(%d) should be(%d)", returned, value)
	}
}

func TestPut10Million(t *testing.T) {

	db := newDB()
	db.setup("/tmp/rocks-test-go")
	defer func() {
		db.Close()
		db.Destroy()
	}()

	limit := 10000000

	key := make([]byte, 4)
	value := make([]byte, 4)
	rand.Read(value)

	now := time.Now()
	for i := 0; i < limit; i++ {
		rand.Read(key)
		db.Put(key, value)

		if i%100000 == 0 {
			fmt.Printf("%d messages inserted\n", i)
		}
	}

	fmt.Printf("%d messages inserted in: %v\n", limit, time.Now().Sub(now))

	acc := 0
	it := db.db.NewIterator(db.ro)
	it.SeekToFirst()
	for it = it; it.Valid(); it.Next() {
		acc++
	}
	if err := it.Err(); err != nil {
		t.Errorf("iterating should not return any error. err: %v", err)
	}
	it.Close()

	fmt.Printf("total number of elements in rocksdb: %d\n", acc)
}
