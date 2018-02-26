package rocks

import (
	"fmt"
	"github.com/chrislusf/gorocksdb"
	"github.com/chrislusf/vasto/pb"
	"sync/atomic"
)

// FullScan scan through all entries
func (d *Rocks) FullScan(batchSize int, fn func([]*pb.RawKeyValue) error) error {
	newClientCounter := atomic.AddInt32(&d.clientCounter, 1)
	defer atomic.AddInt32(&d.clientCounter, -1)
	if newClientCounter <= 0 {
		return ErrorShutdownInProgress
	}

	opts := gorocksdb.NewDefaultReadOptions()
	opts.SetFillCache(false)
	defer opts.Destroy()
	iter := d.db.NewIterator(opts)
	defer iter.Close()

	var rowCount int
	rows := make([]*pb.RawKeyValue, 0, batchSize)
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {

		k := iter.Key()
		v := iter.Value()

		rowCount++
		rows = append(rows, &pb.RawKeyValue{
			Key:   []byte(string(k.Data())),
			Value: []byte(string(v.Data())),
		})
		k.Free()
		v.Free()

		if rowCount%batchSize == 0 {
			err := fn(rows)
			if err != nil {
				return err
			}
			rows = rows[:0]
		}

	}

	if len(rows) > 0 {
		err := fn(rows)
		if err != nil {
			return err
		}
	}

	if err := iter.Err(); err != nil {
		return fmt.Errorf("full scan iterate: %v", err)
	}
	return nil
}
