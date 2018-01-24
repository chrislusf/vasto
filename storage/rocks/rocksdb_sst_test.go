package rocks

import (
	"fmt"
	"log"
	"testing"
	"time"
)

func TestAddNormal(t *testing.T) {

	db := setupTestDb()
	defer cleanup(db)

	now := time.Now()
	limit := 100000

	for i := 0; i < limit; i++ {
		key := []byte(fmt.Sprintf("k%5d", i))
		value := []byte(fmt.Sprintf("v%5d", i))
		db.Put(key, value)
	}

	log.Printf("%d messages inserted by put in: %v\n", limit, time.Now().Sub(now))

}

func TestAddBySst(t *testing.T) {

	db := setupTestDb()
	defer cleanup(db)

	now := time.Now()
	limit := 100000

	for i := 0; i < limit; i++ {
		key := []byte(fmt.Sprintf("k%5d", i))
		value := []byte(fmt.Sprintf("n%5d", i))
		db.Put(key, value)
	}

	i := -1

	var err error

	err = db.addSst("testing1", func() (bool, []byte, []byte) {
		i++
		if i >= limit {
			return false, nil, nil
		}
		key := []byte(fmt.Sprintf("k%5d", i))
		value := []byte(fmt.Sprintf("v%5d", i))
		return true, key, value
	})
	if err != nil {
		t.Errorf("failed to ingest: %v", err)
	}

	log.Printf("%d messages inserted by sst in: %v\n", limit, time.Now().Sub(now))

	if v, err := db.Get([]byte("k12345")); err == nil {
		// this should be returning n12345
		// when allow_ingest_behind is enabled
		if string(v) != "n12345" {
			t.Errorf("get expecting %v, actual %v", "n12345", string(v))
		}
	} else {
		t.Errorf("get by key %v: %v", "k12345", err)
	}

	var counter = count(db)

	if counter != 100000 {
		t.Errorf("scanning expecting %d rows, but actual %d rows", 100000, counter)
	}

	log.Printf("%d messages counted %d: %v\n", limit, counter, time.Now().Sub(now))

	// if overlapping ranges, the addSst will fail
	i, limit = 4, 7
	err = db.addSst("testing2", func() (bool, []byte, []byte) {
		i++
		if i >= limit {
			return false, nil, nil
		}
		key := []byte(fmt.Sprintf("k%5d", i))
		value := []byte(fmt.Sprintf("v%5d", i))
		return true, key, value
	})
	if err == nil {
		t.Errorf("failed to generate ingest error")
	}

	log.Printf("when ingesting SSTable files with over lapping ranges: %v", err)
}
